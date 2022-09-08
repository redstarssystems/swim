(ns org.rssys.swim
  "SWIM functions"
  (:require
    [clojure.set]
    [clojure.spec.alpha :as s]
    [cognitect.transit :as transit]
    [org.rssys.domain :as domain]
    [org.rssys.encrypt :as e]
    [org.rssys.event :as event]
    [org.rssys.scheduler :as scheduler]
    [org.rssys.spec :as spec]
    [org.rssys.udp :as udp]
    [org.rssys.vthread :as vthread])
  (:import
    (clojure.lang
      Keyword
      PersistentHashSet
      PersistentVector)
    (java.io
      ByteArrayInputStream
      ByteArrayOutputStream)
    (java.util
      UUID)
    (org.rssys.domain
      Cluster
      NeighbourNode
      Node
      NodeObject)
    (org.rssys.event
      AckEvent
      AliveEvent
      AntiEntropy
      DeadEvent
      ISwimEvent
      NewClusterSizeEvent
      PingEvent
      ProbeAckEvent
      ProbeEvent)))


;;;;
;; Common functions and constants
;;;;

(def *config
  (atom {:enable-diag-tap?       true                       ;; Put diagnostic data to tap>
         :max-udp-size           1450                       ;; Max size of UDP packet in bytes
         :ignore-max-udp-size?   false                      ;; by default we prevent sending UDP more than :max-udp-size
         :max-payload-size       256                        ;; Max payload size in bytes
         :max-anti-entropy-items 2                          ;; Max items number in anti-entropy
         }))



(defn d>
  "If `*enable-diag-tap?*` is true (default), then put diagnostic data to tap>.
   Returns true if there was room in the tap> queue, false if not (dropped),
   nil if `*enable-diag-tap?*` disabled."
  [cmd-kw node-id data]
  (when (:enable-diag-tap? @*config)
    (tap> {::cmd    cmd-kw
           :node-id node-id
           :data    data})))


(defmacro safe
  [& body]
  `(try
     ~@body
     (catch Exception _#)))


(defn calc-n
  "Calculate how many nodes should we notify.
  n - number of nodes in the cluster."
  [^long n]
  (int (Math/floor (/ (Math/log n) (Math/log 2)))))


(defn serialize
  "Serializes value, returns a byte array"
  [v]
  (let [out    (ByteArrayOutputStream. 1024)
        writer (transit/writer out :msgpack)]
    (transit/write writer v)
    (.toByteArray out)))


(defn deserialize
  "Accepts a byte array, returns deserialized value"
  [^bytes barray]
  (when barray
    (let [in     (ByteArrayInputStream. barray)
          reader (transit/reader in :msgpack)]
      (transit/read reader))))


;;;;;;;;;;;;;;;;;;;;

;;;;
;; Domain entity builders
;;;;

(defn new-cluster
  "Returns new Cluster instance."
  ^Cluster [{:keys [id name desc secret-token nspace tags cluster-size] :as c}]
  (when-not (s/valid? ::spec/cluster c)
    (throw (ex-info "Invalid cluster data" (->> c (s/explain-data ::spec/cluster) spec/problems))))
  (domain/map->Cluster {:id           (or id (random-uuid))
                        :name         name
                        :desc         desc
                        :secret-token secret-token
                        :nspace       nspace
                        :tags         tags
                        :secret-key   (e/gen-secret-key secret-token)
                        :cluster-size (or cluster-size 1)}))


(defn new-neighbour-node
  "Returns new NeighbourNode instance."

  (^NeighbourNode [neighbour-map]
    (if-not (s/valid? ::spec/neighbour-node neighbour-map)
      (throw (ex-info "Invalid neighbour data" (->> neighbour-map (s/explain-data ::spec/neighbour-node) spec/problems)))
      (domain/map->NeighbourNode neighbour-map)))


  (^NeighbourNode [^UUID id ^String host ^long port]
    (new-neighbour-node {:id              id
                         :host            host
                         :port            port
                         :status          :unknown
                         :access          :direct
                         :restart-counter 0
                         :tx              0
                         :payload         {}
                         :updated-at      (System/currentTimeMillis)})))


(defn new-node-object
  "Returns new NodeObject instance."

  (^NodeObject [node-data]
    (when-not (s/valid? ::spec/node node-data)
      (throw (ex-info "Invalid node data" (spec/problems (s/explain-data ::spec/node node-data)))))
    (d> :new-node-object (:id node-data) {:node-data (select-keys node-data [:host :port :status :restart-counter :tx])})
    (domain/map->NodeObject {:*node (atom (domain/map->Node node-data))}))

  (^NodeObject [{:keys [^UUID id ^String host ^long port ^long restart-counter]} ^Cluster cluster]
    (new-node-object {:id                   (or id (random-uuid))
                      :host                 host
                      :port                 port
                      :cluster              cluster
                      :status               :stop
                      :neighbours           {}
                      :restart-counter      (or restart-counter 0)
                      :tx                   0
                      :ping-events          {}               ;; active pings on the fly. we wait ack for them. key ::neighbour-id
                      :outgoing-event-queue []               ;; outgoing events that we'll send to random logN neighbours next time
                      :ping-round-buffer    []               ;; we take logN neighbour ids to send events from event queue
                      :payload              {}               ;; data that this node claims in cluster about itself
                      :scheduler-pool       (scheduler/mk-pool)
                      :*udp-server          nil})))


;;;;;;;;;;;;;;;;;;;;

;;;;
;; NodeObject Getters
;;;;

(defn get-value
  "Get node value"
  ^Node
  [^NodeObject this]
  @(:*node this))


(defn get-id
  "Get node id"
  ^UUID
  [^NodeObject this]
  (.-id (get-value this)))


(defn get-host
  "Get node host"
  ^String
  [^NodeObject this]
  (.-host (get-value this)))


(defn get-port
  "Get node port"
  ^long
  [^NodeObject this]
  (.-port (get-value this)))


(defn get-restart-counter
  "Get node restart counter"
  ^long
  [^NodeObject this]
  (.-restart_counter (get-value this)))


(defn get-tx
  "Get node tx"
  ^long [^NodeObject this]
  (.-tx (get-value this)))


(defn get-cluster
  "Get cluster value"
  ^Cluster
  [^NodeObject this]
  (.-cluster (get-value this)))


(defn get-cluster-size
  "Get cluster size"
  ^long [^NodeObject this]
  (.-cluster_size (get-cluster this)))


(defn get-payload
  "Get node payload"
  [^NodeObject this]
  (:payload (get-value this)))


(defn get-neighbour
  "Get NeighbourNode by id if exists or nil if absent."
  ^NeighbourNode
  [^NodeObject this ^UUID id]
  (get (.-neighbours (get-value this)) id))


(defn get-neighbours
  "Get all node neighbours. Returns map of {:id :neighbour-node}."
  [^NodeObject this]
  (.-neighbours (get-value this)))


(defn get-status
  "Get current node status"
  ^Keyword
  [^NodeObject this]
  (.-status (get-value this)))


(defn get-outgoing-event-queue
  "Get vector of prepared outgoing events"
  [^NodeObject this]
  (.-outgoing_event_queue (get-value this)))


(defn get-ping-events
  "Get map of active ping events which we sent to neighbours"
  [^NodeObject this]
  (.-ping_events (get-value this)))


(defn get-ping-event
  "Get active ping event by neighbour id if exist"
  ^PingEvent
  [^NodeObject this neighbour-id]
  (get (get-ping-events this) neighbour-id))


;;;;;;;;;;;;;;;;;;;;

;;;;
;; NodeObject Setters
;;;;

(defn set-cluster
  "Set new cluster for this node"
  [^NodeObject this ^Cluster cluster]
  (cond
    (not (s/valid? ::spec/cluster cluster))
    (throw (ex-info "Invalid cluster data" (->> cluster (s/explain-data ::spec/cluster) spec/problems)))

    (not= :stop (get-status this))
    (throw (ex-info "Node is not stopped. Can't set new cluster value." {:current-status (get-status this)}))

    :else
    (do
      (d> :set-cluster (get-id this)
        {:cluster (assoc cluster :secret-token "***censored***" :secret-key ["***censored***"])})
      (swap! (:*node this) assoc :cluster cluster))))


(defn set-cluster-size
  "Set new cluster size"
  [^NodeObject this ^long new-cluster-size]
  (when-not (s/valid? ::spec/cluster-size new-cluster-size)
    (throw (ex-info "Invalid cluster size"
             (->> new-cluster-size (s/explain-data ::spec/cluster-size) spec/problems))))
  (d> :set-cluster-size (get-id this) {:new-cluster-size new-cluster-size})
  (swap! (:*node this) assoc :cluster (assoc (get-cluster this) :cluster-size new-cluster-size)))


(defn nodes-in-cluster
  "Returns number of nodes in cluster (neighbours + this). "
  [^NodeObject this]
  (-> this get-neighbours count inc))


(defn cluster-size-exceed?
  "Returns true if cluster size is reached its upper limit."
  [this]
  (let [node-number  (nodes-in-cluster this)
        cluster-size (get-cluster-size this)]
    (>= node-number cluster-size)))


(defn set-status
  "Set new status for this node"
  [^NodeObject this ^Keyword new-status]
  (when-not (s/valid? ::spec/status new-status)
    (throw (ex-info "Invalid node status" (->> new-status (s/explain-data ::spec/status) spec/problems))))
  (d> :set-status (get-id this) {:new-status new-status})
  (swap! (:*node this) assoc :status new-status))


(defn set-payload
  "Set new payload for this node.
  Max size of payload is limited by `*max-payload-size*`."
  [^NodeObject this payload]
  ;;TODO: send event to cluster about new payload
  (when (> (alength ^bytes (serialize payload)) (:max-payload-size @*config))
    (throw (ex-info "Size of payload is too big" {:max-allowed (:max-payload-size @*config)})))
  (d> :set-payload (get-id this) {:payload payload})
  (swap! (:*node this) assoc :payload payload))


(defn set-restart-counter
  "Set node restart counter"
  [^NodeObject this ^long restart-counter]
  (when-not (s/valid? ::spec/restart-counter restart-counter)
    (throw (ex-info "Invalid restart counter data"
             (->> restart-counter (s/explain-data ::spec/restart-counter) spec/problems))))
  (d> :set-restart-counter (get-id this) {:restart-counter restart-counter})
  (swap! (:*node this) assoc :restart-counter restart-counter))


(defn inc-tx
  "Increment node tx"
  [^NodeObject this]
  (swap! (:*node this) assoc :tx (inc (get-tx this))))


(defn upsert-neighbour
  "Update existing or insert new neighbour to neighbours map"
  [^NodeObject this ^NeighbourNode neighbour-node]
  (when-not (s/valid? ::spec/neighbour-node neighbour-node)
    (throw (ex-info "Invalid neighbour node data"
             (->> neighbour-node (s/explain-data ::spec/neighbour-node) spec/problems))))
  (when (cluster-size-exceed? this)
    (d> :upsert-neighbour-cluster-size-exceeded-error (get-id this)
      {:nodes-in-cluster (nodes-in-cluster this)
       :cluster-size     (get-cluster-size this)})
    (throw (ex-info "Cluster size exceeded" {:nodes-in-cluster (nodes-in-cluster this)
                                             :cluster-size     (get-cluster-size this)})))
  (when-not (= (get-id this) (:id neighbour-node))
    (d> :upsert-neighbour (get-id this) {:neighbour-node neighbour-node})
    (swap! (:*node this) assoc :neighbours (assoc
                                             (get-neighbours this)
                                             (.-id neighbour-node)
                                             (assoc neighbour-node :updated-at (System/currentTimeMillis))))))


(defn delete-neighbour
  "Delete neighbour from neighbours map"
  [^NodeObject this ^UUID neighbour-id]
  (d> :delete-neighbour (get-id this) {:neighbour-id neighbour-id})
  (swap! (:*node this) assoc :neighbours (dissoc (get-neighbours this) neighbour-id)))


(defn set-outgoing-event-queue
  "Set outgoing events queue new value"
  [^NodeObject this new-event-queue]
  (when-not (s/valid? ::spec/outgoing-event-queue new-event-queue)
    (throw (ex-info "Invalid outgoing event queue data"
             (->> new-event-queue (s/explain-data ::spec/outgoing-event-queue) spec/problems))))
  (d> :set-outgoing-event-queue (get-id this) {:new-event-queue new-event-queue})
  (swap! (:*node this) assoc :outgoing-event-queue new-event-queue))


(defn put-event
  "Put event to outgoing queue (FIFO)"
  [^NodeObject this ^ISwimEvent event]
  (when-not (instance? ISwimEvent event)
    (throw (ex-info "Event should be instance of ISwimEvent" {:event event})))
  (d> :put-event (get-id this) {:event event :tx (get-tx this)})
  (swap! (:*node this) assoc :outgoing-event-queue
    (conj (get-outgoing-event-queue this) event) :tx (get-tx this)))


(defn take-event
  "Take one event from outgoing queue (FIFO). Returns it.
  Taken event will be removed from queue."
  ^ISwimEvent [^NodeObject this]
  (let [event (first (get-outgoing-event-queue this))]
    (swap! (:*node this) assoc :outgoing-event-queue (->> this get-outgoing-event-queue rest vec))
    event))


;; NB ;; the group-by [:id :restart-counter :tx] and send the latest events only
(defn take-events
  "Take `n`  events from outgoing queue (FIFO). Returns them. If `n` is omitted then take all events.
  Taken events will be removed from queue."
  ([^NodeObject this ^long n]
    (let [events (->> this get-outgoing-event-queue (take n) vec)]
      (swap! (:*node this) assoc :outgoing-event-queue (->> this get-outgoing-event-queue (drop n) vec))
      events))
  ([^NodeObject this]
    (take-events this (count (get-outgoing-event-queue this)))))


(defn upsert-ping
  "Update existing or insert new active ping event in map"
  [^NodeObject this ^PingEvent ping-event]
  (when-not (s/valid? ::spec/ping-event ping-event)
    (throw (ex-info "Invalid ping event data" (->> ping-event (s/explain-data ::spec/ping-event) spec/problems))))
  (d> :upsert-ping (get-id this) {:ping-event ping-event})
  (swap! (:*node this) assoc :ping-events (assoc (get-ping-events this) (:neighbour-id ping-event) ping-event)))


(defn delete-ping
  "Delete active ping event from map"
  [^NodeObject this ^UUID neighbour-id]
  (d> :delete-ping (get-id this) {:neighbour-id neighbour-id})
  (swap! (:*node this) assoc :ping-events (dissoc (get-ping-events this) neighbour-id)))


;;;;;;;;;;;;;;;;;

;;;;
;; Event builders
;;;;

(defn new-ping-event
  "Returns new ping event.
  Params:
  * `ptype` -  :direct or :indirect. Indirect ping is used for investigate suspect neighbours"
  ^PingEvent [^NodeObject this ^UUID neighbour-id attempt-number & {:keys [ptype] :or {ptype :direct}}]
  (let [ping-event (event/map->PingEvent {:cmd-type        (:ping event/code)
                                          :id              (get-id this)
                                          :host            (get-host this)
                                          :port            (get-port this)
                                          :restart-counter (get-restart-counter this)
                                          :tx              (get-tx this)
                                          :neighbour-id    neighbour-id
                                          :attempt-number  attempt-number
                                          :ptype           ptype})]
    (if-not (s/valid? ::spec/ping-event ping-event)
      (throw (ex-info "Invalid ping event" (spec/problems (s/explain-data ::spec/ping-event ping-event))))
      ping-event)))


;;;;

(defn new-ack-event
  "Returns new Ack event"
  ^AckEvent [^NodeObject this ^ISwimEvent e]
  (let [ack-event (event/map->AckEvent {:cmd-type        (:ack event/code)
                                        :id              (get-id this)
                                        :restart-counter (get-restart-counter this)
                                        :tx              (get-tx this)
                                        :neighbour-id    (:id e)
                                        :neighbour-tx    (:tx e)})]
    (if-not (s/valid? ::spec/ack-event ack-event)
      (throw (ex-info "Invalid ack event" (spec/problems (s/explain-data ::spec/ack-event ack-event))))
      ack-event)))


;;;;;

(defn new-dead-event
  "Returns new dead event"
  ^DeadEvent [^NodeObject this ^PingEvent e]
  (let [dead-event (event/map->DeadEvent {:cmd-type        (:dead event/code)
                                          :id              (get-id this)
                                          :restart-counter (get-restart-counter this)
                                          :tx              (get-tx this)
                                          :neighbour-id    (.-id e)
                                          :neighbour-tx    (.-tx e)})]
    (if-not (s/valid? ::spec/dead-event dead-event)
      (throw (ex-info "Invalid dead event" (spec/problems (s/explain-data ::spec/dead-event dead-event))))
      dead-event)))


;;;;

(defn new-probe-event
  "Returns new probe event"
  ^ProbeEvent [^NodeObject this ^String neighbour-host ^long neighbour-port]
  (let [probe-event (event/map->ProbeEvent {:cmd-type        (:probe event/code)
                                            :id              (get-id this)
                                            :host            (get-host this)
                                            :port            (get-port this)
                                            :restart-counter (get-restart-counter this)
                                            :tx              (get-tx this)
                                            :neighbour-host  neighbour-host
                                            :neighbour-port  neighbour-port})]
    (if-not (s/valid? ::spec/probe-event probe-event)
      (throw (ex-info "Invalid probe event" (spec/problems (s/explain-data ::spec/probe-event probe-event))))
      probe-event)))


;;;;



(defn new-probe-ack-event
  "Returns new probe ack event"
  ^ProbeAckEvent [^NodeObject this ^ProbeEvent e]
  (let [ack-event (event/map->ProbeAckEvent {:cmd-type        (:probe-ack event/code)
                                             :id              (get-id this)
                                             :host            (get-host this)
                                             :port            (get-port this)
                                             :status          (get-status this)
                                             :restart-counter (get-restart-counter this)
                                             :tx              (get-tx this)
                                             :neighbour-id    (.-id e)
                                             :neighbour-tx    (.-tx e)})]
    (if-not (s/valid? ::spec/probe-ack-event ack-event)
      (throw (ex-info "Invalid probe ack event" (spec/problems (s/explain-data ::spec/probe-ack-event ack-event))))
      ack-event)))


;;;;

(defn neighbour->vec
  "Convert NeighbourNode to vector of values.
  Field :updated-at is omitted.
  Returns vector of values"
  [^NeighbourNode nb]
  [(:id nb)
   (:host nb)
   (:port nb)
   ((:status nb) event/code)
   (if (= :direct (:access nb)) 0 1)
   (:restart-counter nb)
   (:tx nb)
   (:payload nb)])


(defn vec->neighbour
  "Convert vector of values to NeighbourNode.
  Returns ^NeighbourNode."
  ^NeighbourNode [v]
  (if (= 8 (count v))
    (new-neighbour-node {:id              (nth v 0)
                         :host            (nth v 1)
                         :port            (nth v 2)
                         :status          (get (clojure.set/map-invert event/code) (nth v 3))
                         :access          (if (zero? (nth v 4)) :direct :indirect)
                         :restart-counter (nth v 5)
                         :tx              (nth v 6)
                         :payload         (nth v 7)
                         :updated-at      0})
    (throw (ex-info "Bad NeighbourNode vector" {:v v}))))


(defn build-anti-entropy-data
  "Build anti-entropy data – subset of known nodes from neighbours map.
  This data is propagated from node to node and thus nodes can get knowledge about unknown nodes.
  To apply anti-entropy data receiver should compare incarnation pair [restart-counter tx] and apply only
  if node has older data.
  Returns vector of known neighbors size of `num` if any or empty vector.
  Any item in returned vector is vectorized NeighbourNode."
  [^NodeObject this & {:keys [num] :or {num (:max-anti-entropy-items @*config)}}]
  (or
    (some->>
      (get-neighbours this)
      vals
      shuffle
      (take num)
      (map neighbour->vec)
      vec)
    []))


(defn new-anti-entropy-event
  "Returns anti-entropy event"
  ^AntiEntropy [^NodeObject this]
  (let [anti-entropy-data (build-anti-entropy-data this)
        ae-event          (event/map->AntiEntropy {:cmd-type          (:anti-entropy event/code)
                                                   :anti-entropy-data anti-entropy-data})]
    (if-not (s/valid? ::spec/anti-entropy-event ae-event)
      (throw (ex-info "Invalid anti-entropy event" (spec/problems (s/explain-data ::spec/anti-entropy-event ae-event))))
      ae-event)))


;;;;

(defn new-alive-event
  "Returns new Alive event"
  ^AliveEvent [^NodeObject this ^ISwimEvent e]
  (let [alive-event (event/map->AliveEvent {:cmd-type        (:alive event/code)
                                            :id              (get-id this)
                                            :restart-counter (get-restart-counter this)
                                            :tx              (get-tx this)
                                            :neighbour-id    (:id e)
                                            :neighbour-tx    (:tx e)})]
    (if-not (s/valid? ::spec/alive-event alive-event)
      (throw (ex-info "Invalid alive event" (spec/problems (s/explain-data ::spec/alive-event alive-event))))
      alive-event)))


;;;;

(defn new-cluster-size-event
  "Returns new NewClusterSizeEvent event.
  This event should be created before cluster size changed."
  ^NewClusterSizeEvent [^NodeObject this ^long new-cluster-size]
  (let [ncs-event (event/map->NewClusterSizeEvent {:cmd-type         (:new-cluster-size event/code)
                                                   :id               (get-id this)
                                                   :restart-counter  (get-restart-counter this)
                                                   :tx               (get-tx this)
                                                   :old-cluster-size (get-cluster-size this)
                                                   :new-cluster-size new-cluster-size})]
    (if-not (s/valid? ::spec/new-cluster-size-event ncs-event)
      (throw (ex-info "Invalid new cluster size event" (spec/problems (s/explain-data ::spec/new-cluster-size-event ncs-event))))
      ncs-event)))


;;;;;;;;;;;;;;;;;;;;

;;;;
;; Functions for processing events
;;;;


(defn send-event
  "Send one event to a neighbour.
  Event will be prepared, serialized and encrypted."
  ([^NodeObject this ^ISwimEvent event neighbour-host neighbour-port]
    (let [secret-key     (-> this get-cluster :secret-key)
          prepared-event (.prepare event)
          data           ^bytes (e/encrypt-data secret-key (serialize [prepared-event]))]
      (when (> (alength data) (:max-udp-size @*config))
        (d> :send-event-too-big-udp-error (get-id this) {:udp-size (alength data)})
        (when-not (:ignore-max-udp-size? @*config)
          (throw (ex-info "UDP packet is too big" {:max-allowed (:max-udp-size @*config)}))))
      (udp/send-packet data neighbour-host neighbour-port)))
  ([^NodeObject this ^ISwimEvent event ^UUID neighbour-id]
    (if-let [nb (get-neighbour this neighbour-id)]
      (let [nb-host (.-host nb)
            nb-port (.-port nb)]
        (send-event this event nb-host nb-port))
      (do
        (d> :send-event-unknown-neighbour-id-error (get-id this) {:neighbour-id neighbour-id})
        (throw (ex-info "Unknown neighbour id" {:neighbour-id neighbour-id}))))))


(defn send-event-ae
  "Send one event with attached anti-entropy event to a neighbour.
  Events will be prepared, serialized and encrypted."
  ([^NodeObject this ^ISwimEvent event neighbour-host neighbour-port]
    (let [secret-key         (-> this get-cluster :secret-key)
          prepared-event     (.prepare event)
          anti-entropy-event (.prepare (new-anti-entropy-event this))
          data               ^bytes (e/encrypt-data secret-key (serialize [prepared-event anti-entropy-event]))]
      (when (> (alength data) (:max-udp-size @*config))
        (d> :send-event-ae-too-big-udp-error (get-id this) {:udp-size (alength data)})
        (when-not (:ignore-max-udp-size? @*config)
          (throw (ex-info "UDP packet is too big" {:max-allowed (:max-udp-size @*config)}))))
      (udp/send-packet data neighbour-host neighbour-port)))
  ([^NodeObject this ^ISwimEvent event ^UUID neighbour-id]
    (if-let [nb (get-neighbour this neighbour-id)]
      (let [nb-host (.-host nb)
            nb-port (.-port nb)]
        (send-event-ae this event nb-host nb-port))
      (do
        (d> :send-event-ae-unknown-neighbour-id-error (get-id this) {:neighbour-id neighbour-id})
        (throw (ex-info "Unknown neighbour id" {:neighbour-id neighbour-id}))))))


(defn send-queue-events
  ;; TODO put data or events from queue as param, preparing should be not here
  ;; todo: all events from queue from this node should have the same tx

  "Send all events from outgoing queue with attached anti-entropy event.
   Events will be prepared, serialized and encrypted."
  ([^NodeObject this neighbour-host neighbour-port]
    (let [secret-key             (-> this get-cluster :secret-key)
          events-vector          (take-events this)
          prepared-events-vector (mapv #(.prepare ^ISwimEvent %) events-vector)
          anti-entropy-event     (.prepare (new-anti-entropy-event this))
          events                 (conj prepared-events-vector anti-entropy-event)
          data                   ^bytes (e/encrypt-data secret-key (serialize events))]
      (when (> (alength data) (:max-udp-size @*config))
        (d> :send-queue-events-too-big-udp-error (get-id this) {:udp-size (alength data)})
        (when-not (:ignore-max-udp-size? @*config)
          (throw (ex-info "UDP packet is too big" {:max-allowed (:max-udp-size @*config)}))))
      (d> :send-queue-events-udp-size (get-id this) {:udp-size (alength data)})
      (udp/send-packet data neighbour-host neighbour-port)))
  ([^NodeObject this ^UUID neighbour-id]
    (if-let [nb (get-neighbour this neighbour-id)]
      (let [nb-host (.-host nb)
            nb-port (.-port nb)]
        (send-queue-events this nb-host nb-port))
      (do
        (d> :send-queue-events-unknown-neighbour-id-error (get-id this) {:neighbour-id neighbour-id})
        (throw (ex-info "Unknown neighbour id" {:neighbour-id neighbour-id}))))))


(defn suitable-restart-counter?
  "Check that restart counter from neighbours map is less or equal than from event or neighbour item.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this event-or-neighbour]
  (let [neighbour-id (:id event-or-neighbour)]
    (when-let [nb ^NeighbourNode (get-neighbour this neighbour-id)]
      (<= (.-restart_counter nb) (:restart-counter event-or-neighbour)))))


(defn suitable-tx?
  "Check that tx from neighbours map is less or equal than from event or neighbour item.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this event-or-neighbour]
  (let [neighbour-id (:id event-or-neighbour)]
    (when-let [nb ^NeighbourNode (get-neighbour this neighbour-id)]
      (<= (.-tx nb) (:tx event-or-neighbour)))))


(defn suitable-incarnation?
  "Check that incarnation, pair [restart-counter tx] from neighbours map is less or equal
  than from event or neighbour item.
  Returns true, if suitable and false if not."
  [^NodeObject this event-or-neighbour]
  (= [true true] [(suitable-restart-counter? this event-or-neighbour)
                  (suitable-tx? this event-or-neighbour)]))


(defn get-neighbours-with-status
  "Returns vector of neighbours with desired statuses.
  Params:
    * `status-set`- set of desired statuses. e.g #{:left :alive}"
  [^NodeObject this ^PersistentHashSet status-set]
  (->> this get-neighbours vals (filterv #(status-set (:status %)))))


(defn get-alive-neighbours
  "Returns vector of alive neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:alive}))


(defn get-stopped-neighbours
  "Returns vector of stopped neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:stop}))


(defn get-left-neighbours
  "Returns vector of left neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:left}))


(defn get-dead-neighbours
  "Returns vector of left neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:dead}))


(defn get-suspect-neighbours
  "Returns vector of suspect neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:suspect}))


(defn get-oldest-neighbour
  "Returns the oldest neighbour. If `status-set` is omitted then use all statuses"
  ([^NodeObject this]
    (get-oldest-neighbour this spec/status-set))
  ([^NodeObject this ^PersistentHashSet status-set]
    (let [desired-nb (get-neighbours-with-status this status-set)]
      (->> desired-nb (sort-by :updated-at) first))))


;;;;

(defmulti restore-event (fn [x] (.get ^PersistentVector x 0)))

(defmethod restore-event 0 ^PingEvent [e] (.restore (event/empty-ping) e))
(defmethod restore-event 1 ^AckEvent [e] (.restore (event/empty-ack) e))
(defmethod restore-event 8 ^AntiEntropy [e] (.restore (event/empty-anti-entropy) e))
(defmethod restore-event 9 ^ProbeEvent [e] (.restore (event/empty-probe) e))
(defmethod restore-event 10 ^ProbeAckEvent [e] (.restore (event/empty-probe-ack) e))



(defmulti process-incoming-event (fn [this e] (type e)))


(defmethod process-incoming-event ProbeEvent
  [^NodeObject this ^ProbeEvent e]
  (let [probe-ack-event (new-probe-ack-event this e)]
    (inc-tx this)                                           ;; every new event on node increments tx
    (d> :probe-ack-event (get-id this) probe-ack-event)
    (send-event this probe-ack-event (.-host e) (.-port e))))


(defmethod process-incoming-event ProbeAckEvent
  [^NodeObject this ^ProbeAckEvent e]
  (let [nb (new-neighbour-node {:id              (.-id e)
                                :host            (.-host e)
                                :port            (.-port e)
                                :status          (.-status e)
                                :access          :direct
                                :restart-counter (.-restart_counter e)
                                :tx              (.-tx e)
                                :payload         {}
                                :updated-at      (System/currentTimeMillis)})]
    (d> :probe-ack-event (get-id this) nb)
    (upsert-neighbour this nb)))


(defmethod process-incoming-event AckEvent
  [^NodeObject this ^AckEvent e]
  (let [neighbour-id (:id e)
        nb           (get-neighbour this neighbour-id)]
    (cond

      ;; do nothing if event from unknown node
      (nil? nb)
      (d> :ack-event-unknown-neighbour-error (get-id this) e)

      ;; do nothing if event with outdated restart counter
      (not (suitable-restart-counter? this e))
      (d> :ack-event-bad-restart-counter-error (get-id this) e)

      ;; do nothing if event with outdated tx
      (not (suitable-tx? this e))
      (d> :ack-event-bad-tx-error (get-id this) e)

      ;; do not process events from not alive nodes
      (not (#{:alive :suspect} (:status nb)))
      (d> :ack-event-not-alive-neighbour-error (get-id this) e)

      ;; do nothing if ack is not requested
      (nil? (get-ping-event this (:id e)))
      (d> :ack-event-no-active-ping-error (get-id this) e)


      :else (do
              ;; here we work only with alive and suspect nodes
              (delete-ping this neighbour-id)
              (upsert-neighbour this (assoc nb :status :alive))
              (when (= :suspect (:status nb))
                (put-event this (new-alive-event this e))
                (inc-tx this)
                (d> :alive-event (get-id this) {:neighbour-id neighbour-id :previous-status :suspect}))
              (d> :ack-event (get-id this) e)))))


(defmethod process-incoming-event AntiEntropy
  [^NodeObject this ^AntiEntropy e]
  (let [neighbour-vec (->> e (.-anti_entropy_data) (mapv vec->neighbour))]
    (doseq [ae-neighbour neighbour-vec]
      (if (get-neighbour this (:id ae-neighbour))
        (when (suitable-incarnation? this ae-neighbour)
          (d> :anti-entropy-event (get-id this) ae-neighbour)
          (upsert-neighbour this ae-neighbour))
        (when-not (= (get-id this) (:id ae-neighbour))      ;; we don't want to put itself to neighbour map
          (d> :anti-entropy-event (get-id this) ae-neighbour)
          (upsert-neighbour this ae-neighbour))))))


(defmethod process-incoming-event PingEvent
  [^NodeObject this ^PingEvent e]

  ;; add new neighbour if it not exists in neighbours map
  ;; TODO  переделать на проверку статуса dead т.к. это нарушает стейт-машину (нарисовать!)
  (when (not (get-neighbour this (.-id e)))
    (let [new-neighbour (new-neighbour-node {:id              (.-id e)
                                             :host            (.-host e)
                                             :port            (.-port e)
                                             :status          :alive
                                             :access          :direct
                                             :restart-counter (.-restart_counter e)
                                             :tx              (.-tx e)
                                             :payload         {}
                                             :updated-at      (System/currentTimeMillis)})]

      (d> :process-incoming-event-ping-add-new-neighbour (get-id this) new-neighbour)
      (upsert-neighbour this new-neighbour)))

  (cond

    (not (suitable-restart-counter? this e))
    (let [dead-event (new-dead-event this e)]
      (inc-tx this)                                         ;; every event on node increments tx
      (d> :process-incoming-event-ping-dead-event (get-id this) dead-event)
      (send-event this dead-event (.-host e) (.-port e)))

    (not (suitable-tx? this e)) :do-nothing

    :else
    (let [ack-event         (new-ack-event this e)
          anti-entropy-data :todo]
      (inc-tx this)                                         ;; every event on node increments tx
      (d> :process-incoming-event-ping-ack-event (get-id this) ack-event)
      (send-event-ae this ack-event (.-host e) (.-port e))))


  ;; добавить новое событие probe
  ;; не добавлять узел при обработке пинг, т.к. узлы должны заходить через событие join
  ;; проверить, что узел не имел предыдущий статус dead
  ;; обновить tx у neighbour в таблице соседей значением из пришедшего события.
  ;; сформировать вектор из [ack event + все текущие исходящие события + антиэнтропия] но не более событий чем может принять udp пакет.
  ;; отправить ack event немедленно.
  ;; установить статус соседа как :alive

  #_(when-let [nb ^NeighbourNode (.neighbour this (.-id e))]
      (when (not=
              [(.-host e) (.-port e)]
              [(.-host nb) (.-port nb)])
        ())
      (.upsert_neighbour this (new-neighbour-node {:id              (.-id e)
                                                   :host            (.-host e)
                                                   :port            (.-port e)
                                                   :status          :alive
                                                   :access          :direct
                                                   :restart-counter (.-restart_counter e)
                                                   :tx              (.-tx e)
                                                   :payload         {}
                                                   :updated-at      (System/currentTimeMillis)})))
  #_(let [neighbour-id (.-id e)                             ;; от кого получили пинг

          ])
  (get-neighbours this)
  (println (get-tx this)))



(defmethod process-incoming-event :default
  [^NodeObject this e]
  (d> :process-incoming-event-default (get-id this) {:msg "Unknown event type" :event e}))


;;;;


(defn incoming-udp-processor-fn
  "Main function to process all incoming UDP packets. UDP packets will be decrypted, serialized to events.
  Events will be processed one by one and dispatched to corresponding event handler.
  Returns void."
  [^NodeObject this ^bytes encrypted-data]
  (let [secret-key     (-> this get-cluster :secret-key)
        decrypted-data (safe (e/decrypt-data ^bytes secret-key ^bytes encrypted-data)) ;; Ignore bad messages
        events-vector  (deserialize ^bytes decrypted-data)]
    (if (vector? events-vector)
      (doseq [serialized-event events-vector]
        (let [event (restore-event serialized-event)]
          (inc-tx this)                                     ;; Every incoming event must increment tx on this node
          (d> :incoming-udp-processor (get-id this) {:event event})
          (process-incoming-event this event)))
      (d> :incoming-udp-processor-error (get-id this) {:msg "Bad events vector structure" :events-vector events-vector}))))


(defn node-process-fn
  [^NodeObject this]
  (let [*idx (atom 0)
        rot  ["\\" "|" "/" "—"]]
    (Thread/sleep 100)
    (while (-> this get-value :*udp-server deref :continue?)
      (print (format "\rNode is active: %s  " (nth rot (rem @*idx (count rot)))))
      (Thread/sleep 200)
      (swap! *idx inc)
      (flush)))
  (println "Node is stopped."))


;;;;

;;;;;;;;;;;;;;;;;;;;

;;;;
;; NodeObject Operations
;;;;

(defn start
  "Start the node and run `node-process-fn` in a separate virtual thread.
   Params:
    * `node-process-fn` - fn with one arg `this` for main node process. It may look for :continue? flag in UDP server.
    * `incoming-data-processor-fn` fn to process incoming UDP packets with two args: `this` and `encrypted-data`"
  [^NodeObject this node-process-fn incoming-data-processor-fn]
  (when (= (get-status this) :stop)
    (set-status this :left)
    (set-restart-counter this (inc (get-restart-counter this)))
    (let [{:keys [host port]} (get-value this)]
      (swap! (:*node this) assoc :*udp-server (udp/start host port (partial incoming-data-processor-fn this))))
    (when-not (s/valid? ::spec/node (get-value this))
      (throw (ex-info "Invalid node data" (->> this :*node (s/explain-data ::spec/node) spec/problems))))
    (vthread/vfuture (node-process-fn this))
    (d> :start (get-id this) {})))


(defn leave
  "Leave the cluster"
  [^NodeObject this]
  ;;TODO
  )


;; TODO:
;; How to clean neighbour table from old nodes?
;;


(defn join
  "Join this node to the cluster"
  [^NodeObject this]
  ;;TODO
  )


(defn probe
  "Probe other node.
  If is responds then put other node to a neighbours table if cluster size is not exceeded."
  [^NodeObject this ^String neighbour-host ^long neighbour-port]
  (let [probe-event (new-probe-event this neighbour-host neighbour-port)]
    (inc-tx this)                                           ;; every new event should increase tx
    (d> :probe (get-id this) probe-event)
    (send-event this probe-event neighbour-host neighbour-port)))


;; NB: if in Ack id is different, then send event and change id in a neighbours table
(defn ping
  "Send Ping event to neighbour node"
  [^NodeObject this neighbour-id]
  ;;TODO
  )


(defn stop
  "Stop the node and leave the cluster"
  [^NodeObject this]
  (let [{:keys [*udp-server scheduler-pool]} (get-value this)]
    (leave this)
    (scheduler/stop-and-reset-pool! scheduler-pool :strategy :kill)
    (swap! (:*node this) assoc
      :*udp-server (udp/stop *udp-server)
      :ping-events {}
      :outgoing-event-queue []
      :ping-round-buffer []
      :tx 0)
    (set-status this :stop)
    (when-not (s/valid? ::spec/node (get-value this))
      (throw (ex-info "Invalid node data" (spec/problems (s/explain-data ::spec/node (:*node this)))))))
  (d> :stop (get-id this) {}))


;;;;;;;;;;;;;;;;;;;;
