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
      IndirectAckEvent
      IndirectPingEvent
      JoinEvent
      LeftEvent
      NewClusterSizeEvent
      PayloadEvent
      PingEvent
      ProbeAckEvent
      ProbeEvent
      SuspectEvent)))


;;;;
;; Common functions and constants
;;;;

(def *config
  (atom {:enable-diag-tap?                    true          ;; Put diagnostic data to tap>
         :max-udp-size                        1432          ;; Max size of UDP packet in bytes
         :ignore-max-udp-size?                false         ;; by default, we prevent sending UDP more than :max-udp-size
         :max-payload-size                    256           ;; Max payload size in bytes
         :max-anti-entropy-items              2             ;; Max items number in anti-entropy
         :max-ping-without-ack-before-suspect 2             ;; How many pings without ack before node became suspect
         :max-ping-without-ack-before-dead    4             ;; How many pings without ack before node considered as dead

         :ping-heartbeat-ms                   300           ;; Send ping+events to neighbours every N ms
         }))


(def *stat
  (atom {:bad-udp-counter 0                                 ;; how many UDP packet we received are bad
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
                      :ping-events          {}               ;; active direct pings
                      :indirect-ping-events {}               ;; active indirect pings
                      :payload              {}               ;; data that this node claims in cluster about itself
                      :scheduler-pool       (scheduler/mk-pool)
                      :*udp-server          nil
                      :outgoing-events      []               ;; outgoing events that we'll send to random logN neighbours next time
                      :ping-round-buffer    []               ;; we take logN neighbour ids to send events from event queue
                      :probe-events         {}               ;; outgoing probe events
                      })))


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


(defn get-cluster
  "Get cluster value"
  ^Cluster
  [^NodeObject this]
  (.-cluster (get-value this)))


(defn get-cluster-size
  "Get cluster size"
  ^long [^NodeObject this]
  (.-cluster_size (get-cluster this)))


(defn get-status
  "Get current node status"
  ^Keyword
  [^NodeObject this]
  (.-status (get-value this)))


(defn get-neighbours
  "Get all node neighbours. Returns map of {:id :neighbour-node}."
  [^NodeObject this]
  (.-neighbours (get-value this)))


(defn get-neighbour
  "Get NeighbourNode by id if exists or nil if absent."
  ^NeighbourNode
  [^NodeObject this ^UUID id]
  (get (.-neighbours (get-value this)) id))


(defn get-restart-counter
  "Get node restart counter"
  ^long
  [^NodeObject this]
  (.-restart_counter (get-value this)))


(defn get-tx
  "Get node tx"
  ^long [^NodeObject this]
  (.-tx (get-value this)))


(defn get-ping-events
  "Get map of active ping events which we sent to neighbours"
  [^NodeObject this]
  (.-ping_events (get-value this)))


(defn get-ping-event
  "Get active ping event by neighbour id if exist"
  ^PingEvent
  [^NodeObject this neighbour-id]
  (get (get-ping-events this) neighbour-id))


(defn get-indirect-ping-events
  "Get map of active indirect ping events which we received from neighbours"
  [^NodeObject this]
  (.-indirect_ping_events (get-value this)))


(defn get-indirect-ping-event
  "Get active indirect ping event by keyword  if exist"
  ^PingEvent
  [^NodeObject this ^Keyword indirect-id]
  (get (get-indirect-ping-events this) indirect-id))


(defn get-payload
  "Get node payload"
  [^NodeObject this]
  (:payload (get-value this)))


(defn get-outgoing-events
  "Get vector of prepared outgoing events"
  [^NodeObject this]
  (.-outgoing_events (get-value this)))


(defn get-ping-round-buffer
  "Get vector of neighbour ids"
  [^NodeObject this]
  (.-ping_round_buffer (get-value this)))


(defn get-probe-events
  "Get map of probe events which we received from neighbours"
  [^NodeObject this]
  (.-probe_events (get-value this)))


(defn get-probe-event
  "Get active probe event if exist"
  ^PingEvent
  [^NodeObject this ^UUID probe-key]
  (get (get-probe-events this) probe-key))


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


(defn set-outgoing-events
  "Set outgoing events to new value"
  [^NodeObject this new-outgoing-events]
  (when-not (s/valid? ::spec/outgoing-events new-outgoing-events)
    (throw (ex-info "Invalid outgoing events data"
             (->> new-outgoing-events
               (s/explain-data ::spec/outgoing-events) spec/problems))))
  (d> :set-outgoing-events (get-id this) {:new-outgoing-events new-outgoing-events})
  (swap! (:*node this) assoc :outgoing-events new-outgoing-events))


(defn put-event
  "Put event to outgoing queue (FIFO)"
  [^NodeObject this ^ISwimEvent event]
  (when-not (instance? ISwimEvent event)
    (throw (ex-info "Event should be instance of ISwimEvent" {:event event})))
  (d> :put-event (get-id this) {:event event :tx (get-tx this)})
  (swap! (:*node this) assoc :outgoing-events
    (conj (get-outgoing-events this) event) :tx (get-tx this)))


;; NB ;; the group-by [:id :restart-counter :tx] and send the latest events only
(defn take-events
  "Take `n`  outgoing events from FIFO buffer and return them.
   If `n` is omitted then take all events.
   Taken events will be removed from buffer."
  ([^NodeObject this ^long n]
    (let [events (->> this get-outgoing-events (take n) vec)]
      (swap! (:*node this) assoc :outgoing-events (->> this get-outgoing-events (drop n) vec))
      events))
  ([^NodeObject this]
    (take-events this (count (get-outgoing-events this)))))


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


(defn upsert-indirect-ping
  "Update existing or insert new active indirect ping event in map"
  [^NodeObject this ^IndirectPingEvent indirect-ping-event]
  (when-not (s/valid? ::spec/indirect-ping-event indirect-ping-event)
    (throw (ex-info "Invalid indirect ping event data"
             (->> indirect-ping-event (s/explain-data ::spec/indirect-ping-event) spec/problems))))
  (let [indirect-id (.-neighbour_id indirect-ping-event)]
    (d> :upsert-indirect-ping (get-id this) {:indirect-ping-event indirect-ping-event
                                             :indirect-id         indirect-id})
    (swap! (:*node this) assoc :indirect-ping-events
      (assoc (get-indirect-ping-events this) indirect-id indirect-ping-event))))


(defn delete-indirect-ping
  "Delete active ping event from map"
  [^NodeObject this indirect-id]
  (d> :delete-indirect-ping (get-id this) {:indirect-id indirect-id})
  (swap! (:*node this) assoc :indirect-ping-events (dissoc (get-indirect-ping-events this) indirect-id)))


(defn insert-probe
  "Insert new active probe event in map {probe-key nil}
  `:probe-key` is used as a key in event map."
  [^NodeObject this ^ProbeEvent probe-event]
  (when-not (s/valid? ::spec/probe-event probe-event)
    (throw (ex-info "Invalid probe event data" (->> probe-event (s/explain-data ::spec/probe-event) spec/problems))))
  (d> :upsert-probe (get-id this) {:probe-event probe-event})
  (swap! (:*node this) assoc :probe-events (assoc (get-probe-events this) (.-probe_key probe-event) nil))
  (.-probe_key probe-event))


(defn delete-probe
  "Delete active probe event from map"
  [^NodeObject this ^UUID probe-key]
  (d> :delete-probe (get-id this) {:neighbour-id probe-key})
  (swap! (:*node this) assoc :probe-events (dissoc (get-probe-events this) probe-key)))


(defn upsert-probe-ack
  "Update existing or insert new probe ack event in map.
  `:probe-key` is used as a key in event map."
  [^NodeObject this ^ProbeAckEvent probe-ack-event]
  (when-not (s/valid? ::spec/probe-ack-event probe-ack-event)
    (throw (ex-info "Invalid probe ack event data" (->> probe-ack-event (s/explain-data ::spec/probe-ack-event) spec/problems))))
  (d> :upsert-probe-ack (get-id this) {:probe-ack-event probe-ack-event})
  (swap! (:*node this) assoc :probe-events (assoc (get-probe-events this) (.-probe_key probe-ack-event) probe-ack-event))
  (.-probe_key probe-ack-event))


;;;;;;;;;;;;;;;;;

;;;;
;; Event builders
;;;;


(defn new-probe-event
  "Returns new probe event. Increments tx of `this` node."
  ^ProbeEvent [^NodeObject this ^String neighbour-host ^long neighbour-port]
  (let [probe-event (event/map->ProbeEvent {:cmd-type        (:probe event/code)
                                            :id              (get-id this)
                                            :host            (get-host this)
                                            :port            (get-port this)
                                            :restart-counter (get-restart-counter this)
                                            :tx              (get-tx this)
                                            :neighbour-host  neighbour-host
                                            :neighbour-port  neighbour-port
                                            :probe-key       (UUID/randomUUID)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/probe-event probe-event)
      (throw (ex-info "Invalid probe event" (spec/problems (s/explain-data ::spec/probe-event probe-event))))
      probe-event)))


;;;;



(defn new-probe-ack-event
  "Returns new probe ack event. Increments tx of `this` node."
  ^ProbeAckEvent [^NodeObject this ^ProbeEvent e]
  (let [ack-event (event/map->ProbeAckEvent {:cmd-type        (:probe-ack event/code)
                                             :id              (get-id this)
                                             :host            (get-host this)
                                             :port            (get-port this)
                                             :status          (get-status this)
                                             :restart-counter (get-restart-counter this)
                                             :tx              (get-tx this)
                                             :neighbour-id    (.-id e)
                                             :neighbour-tx    (.-tx e)
                                             :probe-key       (.-probe_key e)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/probe-ack-event ack-event)
      (throw (ex-info "Invalid probe ack event" (spec/problems (s/explain-data ::spec/probe-ack-event ack-event))))
      ack-event)))


;;;;


(defn new-ping-event
  "Returns new ping event. Increments tx of `this` node."
  ^PingEvent [^NodeObject this ^UUID neighbour-id attempt-number]
  (let [ping-event (event/map->PingEvent {:cmd-type        (:ping event/code)
                                          :id              (get-id this)
                                          :host            (get-host this)
                                          :port            (get-port this)
                                          :restart-counter (get-restart-counter this)
                                          :tx              (get-tx this)
                                          :neighbour-id    neighbour-id
                                          :attempt-number  attempt-number})]
    (inc-tx this)
    (if-not (s/valid? ::spec/ping-event ping-event)
      (throw (ex-info "Invalid ping event" (spec/problems (s/explain-data ::spec/ping-event ping-event))))
      ping-event)))


;;;;

(defn new-ack-event
  "Returns new Ack event. Increments tx of `this` node."
  ^AckEvent [^NodeObject this ^ISwimEvent e]
  (let [ack-event (event/map->AckEvent {:cmd-type        (:ack event/code)
                                        :id              (get-id this)
                                        :restart-counter (get-restart-counter this)
                                        :tx              (get-tx this)
                                        :neighbour-id    (:id e)
                                        :neighbour-tx    (:tx e)
                                        :attempt-number  (:attempt-number e)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/ack-event ack-event)
      (throw (ex-info "Invalid ack event" (spec/problems (s/explain-data ::spec/ack-event ack-event))))
      ack-event)))


;;;;


(defn new-indirect-ping-event
  "Returns new indirect ping event. Increments tx of `this` node."
  ^IndirectPingEvent [^NodeObject this ^UUID intermediate-id ^UUID neighbour-id attempt-number]
  (let [intermediate (get-neighbour this intermediate-id)
        neighbour    (get-neighbour this neighbour-id)]
    (cond

      (nil? intermediate)
      (throw (ex-info "Unknown intermediate node with such id" {:intermediate-id intermediate-id}))

      (nil? neighbour)
      (throw (ex-info "Unknown neighbour node with such id" {:neighbour-id neighbour-id}))

      :else
      (let [indirect-ping-event
            (event/map->IndirectPingEvent {:cmd-type          (:indirect-ping event/code)
                                           :id                (get-id this)
                                           :host              (get-host this)
                                           :port              (get-port this)
                                           :restart-counter   (get-restart-counter this)
                                           :tx                (get-tx this)
                                           :intermediate-id   intermediate-id
                                           :intermediate-host (:host intermediate)
                                           :intermediate-port (:port intermediate)
                                           :neighbour-id      neighbour-id
                                           :neighbour-host    (:host neighbour)
                                           :neighbour-port    (:port neighbour)
                                           :attempt-number    attempt-number})]
        (inc-tx this)
        (if-not (s/valid? ::spec/indirect-ping-event indirect-ping-event)
          (throw (ex-info "Invalid indirect ping event"
                   (spec/problems (s/explain-data ::spec/indirect-ping-event indirect-ping-event))))
          indirect-ping-event)))))


;;;;

(defn new-indirect-ack-event
  "Returns new indirect ack event. Increments tx of `this` node."
  ^IndirectAckEvent [^NodeObject this ^IndirectPingEvent e]
  (let [indirect-ack-event
        (event/map->IndirectAckEvent {:cmd-type          (:indirect-ack event/code)
                                      :id                (get-id this)
                                      :host              (get-host this)
                                      :port              (get-port this)
                                      :restart-counter   (get-restart-counter this)
                                      :tx                (get-tx this)
                                      :status            (get-status this)
                                      :intermediate-id   (.-intermediate_id e)
                                      :intermediate-host (.-intermediate_host e)
                                      :intermediate-port (.-intermediate_port e)
                                      :neighbour-id      (.-id e)
                                      :neighbour-host    (.-host e)
                                      :neighbour-port    (.-port e)
                                      :attempt-number    (.-attempt_number e)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/indirect-ack-event indirect-ack-event)
      (throw (ex-info "Invalid indirect ack event"
               (spec/problems (s/explain-data ::spec/indirect-ack-event indirect-ack-event))))
      indirect-ack-event)))


;;;;

(defn new-alive-event
  "Returns new Alive event. Increments tx of `this` node."
  ^AliveEvent [^NodeObject this ^ISwimEvent e]
  (let [alive-event
        (event/map->AliveEvent {:cmd-type                  (:alive event/code)
                                :id                        (get-id this)
                                :restart-counter           (get-restart-counter this)
                                :tx                        (get-tx this)
                                :neighbour-id              (:id e)
                                :neighbour-restart-counter (:restart-counter e)
                                :neighbour-tx              (:tx e)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/alive-event alive-event)
      (throw (ex-info "Invalid alive event"
               (spec/problems (s/explain-data ::spec/alive-event alive-event))))
      alive-event)))


;;;;;

(defn new-dead-event
  "Returns new dead event. Increments tx of `this` node."
  ^DeadEvent [^NodeObject this ^UUID neighbour-id]
  (let [nb                 (get-neighbour this neighbour-id)
        nb-restart-counter (:restart-counter nb)
        nb-tx              (:tx nb)
        dead-event
        (event/map->DeadEvent {:cmd-type                  (:dead event/code)
                               :id                        (get-id this)
                               :restart-counter           (get-restart-counter this)
                               :tx                        (get-tx this)
                               :neighbour-id              neighbour-id
                               :neighbour-restart-counter nb-restart-counter
                               :neighbour-tx              nb-tx})]
    (inc-tx this)
    (if-not (s/valid? ::spec/dead-event dead-event)
      (throw (ex-info "Invalid dead event"
               (spec/problems (s/explain-data ::spec/dead-event dead-event))))
      dead-event)))


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
  "Returns anti-entropy event. Increments tx of `this` node."
  ^AntiEntropy [^NodeObject this]
  (let [anti-entropy-data (build-anti-entropy-data this)
        ae-event          (event/map->AntiEntropy {:cmd-type          (:anti-entropy event/code)
                                                   :id                (get-id this)
                                                   :restart-counter   (get-restart-counter this)
                                                   :tx                (get-tx this)
                                                   :anti-entropy-data anti-entropy-data})]
    (inc-tx this)
    (if-not (s/valid? ::spec/anti-entropy-event ae-event)
      (throw (ex-info "Invalid anti-entropy event" (spec/problems (s/explain-data ::spec/anti-entropy-event ae-event))))
      ae-event)))


;;;;

;; TODO: where to check if new cluster size is less than active nodes in a cluster?
(defn new-cluster-size-event
  "Returns new NewClusterSizeEvent event. Increments tx of `this` node.
  This event should be created before cluster size changed."
  ^NewClusterSizeEvent [^NodeObject this ^long new-cluster-size]
  (let [ncs-event (event/map->NewClusterSizeEvent {:cmd-type         (:new-cluster-size event/code)
                                                   :id               (get-id this)
                                                   :restart-counter  (get-restart-counter this)
                                                   :tx               (get-tx this)
                                                   :old-cluster-size (get-cluster-size this)
                                                   :new-cluster-size new-cluster-size})]
    (inc-tx this)
    (if-not (s/valid? ::spec/new-cluster-size-event ncs-event)
      (throw (ex-info "Invalid new cluster size event" (spec/problems (s/explain-data ::spec/new-cluster-size-event ncs-event))))
      ncs-event)))


;;;;


(defn new-join-event
  "Returns new JoinEvent event. Increments tx of `this` node."
  ^JoinEvent [^NodeObject this]
  (let [join-event (event/map->JoinEvent {:cmd-type        (:join event/code)
                                          :id              (get-id this)
                                          :restart-counter (get-restart-counter this)
                                          :tx              (get-tx this)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/join-event join-event)
      (throw (ex-info "Invalid join event data"
               (spec/problems (s/explain-data ::spec/join-event join-event))))
      join-event)))


;;;;;

(defn new-suspect-event
  "Returns new suspect event. Increments tx of `this` node."
  ^SuspectEvent [^NodeObject this ^UUID neighbour-id]
  (let [nb                 (get-neighbour this neighbour-id)
        nb-restart-counter (:restart-counter nb)
        nb-tx              (:tx nb)
        suspect-event
        (event/map->SuspectEvent {:cmd-type                  (:suspect event/code)
                                  :id                        (get-id this)
                                  :restart-counter           (get-restart-counter this)
                                  :tx                        (get-tx this)
                                  :neighbour-id              neighbour-id
                                  :neighbour-restart-counter nb-restart-counter
                                  :neighbour-tx              nb-tx})]
    (inc-tx this)
    (if-not (s/valid? ::spec/suspect-event suspect-event)
      (throw (ex-info "Invalid suspect event data"
               (spec/problems (s/explain-data ::spec/suspect-event suspect-event))))
      suspect-event)))


;;;;

(defn new-left-event
  "Returns new LeftEvent event. Increments tx of `this` node."
  ^LeftEvent [^NodeObject this]
  (let [e (event/map->LeftEvent {:cmd-type        (:left event/code)
                                 :id              (get-id this)
                                 :restart-counter (get-restart-counter this)
                                 :tx              (get-tx this)})]
    (inc-tx this)
    e))


;;;;


(defn new-payload-event
  "Returns new PayloadEvent event. Increments tx of `this` node."
  ^PayloadEvent [^NodeObject this]
  (let [e (event/map->PayloadEvent {:cmd-type        (:payload event/code)
                                    :id              (get-id this)
                                    :restart-counter (get-restart-counter this)
                                    :tx              (get-tx this)
                                    :payload         (get-payload this)})]
    (inc-tx this)
    e))


;;;;;;;;;;;;;;;;;;;;

;;;;
;; Functions for sending events
;;;;

(defn send-events
  "Send given events to a neighbour.
   Events will be prepared, serialized and encrypted."
  [^NodeObject this events-vector neighbour-host neighbour-port]
  (let [secret-key             (-> this get-cluster :secret-key)
        prepared-events-vector (mapv #(.prepare ^ISwimEvent %) events-vector)
        data                   ^bytes (e/encrypt-data secret-key (serialize prepared-events-vector))]
    (when (> (alength data) (:max-udp-size @*config))
      (d> :send-events-too-big-udp-error (get-id this) {:udp-size (alength data)})
      (when-not (:ignore-max-udp-size? @*config)
        (throw (ex-info "UDP packet is too big" {:max-allowed (:max-udp-size @*config)}))))
    (d> :send-events-udp-size (get-id this) {:udp-size (alength data)})
    (udp/send-packet data neighbour-host neighbour-port)))


(defn send-event
  "Send one event to a neighbour.
  Event will be prepared, serialized and encrypted."
  ([^NodeObject this ^ISwimEvent event neighbour-host neighbour-port]
    (send-events this [event] neighbour-host neighbour-port))
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
    (send-events this [event (new-anti-entropy-event this)] neighbour-host neighbour-port))
  ([^NodeObject this ^ISwimEvent event ^UUID neighbour-id]
    (if-let [nb (get-neighbour this neighbour-id)]
      (let [nb-host (.-host nb)
            nb-port (.-port nb)]
        (send-event-ae this event nb-host nb-port))
      (do
        (d> :send-event-ae-unknown-neighbour-id-error (get-id this) {:neighbour-id neighbour-id})
        (throw (ex-info "Unknown neighbour id" {:neighbour-id neighbour-id}))))))


;;;;;;;;;;;;;;;;;;;;

;;;;
;; Helper functions
;;;;


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
      (> (:tx event-or-neighbour) (.-tx nb)))))


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
  "Returns the oldest neighbour by :updated-at attribute. If `status-set` is omitted then use all statuses"
  ([^NodeObject this]
    (get-oldest-neighbour this spec/status-set))
  ([^NodeObject this ^PersistentHashSet status-set]
    (let [desired-nb (get-neighbours-with-status this status-set)]
      (->> desired-nb (sort-by :updated-at) first))))

(defn alive-neighbour?
  "Returns true if neighbour has alive statuses."
  [^NeighbourNode nb]
  (boolean (#{:alive :suspect} (:status nb))))


;;;;;;;;;;;;;;;;;;;;

;;;;
;; Functions for processing events
;;;;


(defmulti restore-event (fn [x] (.get ^PersistentVector x 0)))

(defmethod restore-event 0 ^PingEvent [e] (.restore (event/empty-ping) e))
(defmethod restore-event 1 ^AckEvent [e] (.restore (event/empty-ack) e))
(defmethod restore-event 2 ^JoinEvent [e] (.restore (event/empty-join) e))
(defmethod restore-event 3 ^AliveEvent [e] (.restore (event/empty-alive) e))
(defmethod restore-event 4 ^SuspectEvent [e] (.restore (event/empty-suspect) e))
(defmethod restore-event 5 ^LeftEvent [e] (.restore (event/empty-left) e))
(defmethod restore-event 6 ^DeadEvent [e] (.restore (event/empty-dead) e))
(defmethod restore-event 7 ^PayloadEvent [e] (.restore (event/empty-payload) e))
(defmethod restore-event 8 ^AntiEntropy [e] (.restore (event/empty-anti-entropy) e))
(defmethod restore-event 9 ^ProbeEvent [e] (.restore (event/empty-probe) e))
(defmethod restore-event 10 ^ProbeAckEvent [e] (.restore (event/empty-probe-ack) e))
(defmethod restore-event 13 ^NewClusterSizeEvent [e] (.restore (event/empty-new-cluster-size) e))
(defmethod restore-event 14 ^IndirectPingEvent [e] (.restore (event/empty-indirect-ping) e))
(defmethod restore-event 15 ^IndirectAckEvent [e] (.restore (event/empty-indirect-ack) e))


(defmulti process-incoming-event (fn [this e] (type e)))


(defmethod process-incoming-event ProbeEvent
  [^NodeObject this ^ProbeEvent e]
  (let [probe-ack-event (new-probe-ack-event this e)]
    (d> :probe-ack-event (get-id this) probe-ack-event)
    (send-event this probe-ack-event (.-host e) (.-port e))))


(defmethod process-incoming-event ProbeAckEvent
  [^NodeObject this ^ProbeAckEvent e]
  (let [probe-key (.-probe_key e)
        nb
        (new-neighbour-node {:id              (.-id e)
                             :host            (.-host e)
                             :port            (.-port e)
                             :status          (.-status e)
                             :access          :direct
                             :restart-counter (.-restart_counter e)
                             :tx              (.-tx e)
                             :payload         {}
                             :updated-at      (System/currentTimeMillis)})]
    (d> :probe-ack-event (get-id this) nb)

    (if (and (contains? (get-probe-events this) probe-key) ;; check  we send probe-event before
          (= (.-neighbour_id e) (get-id this)))           ;; this probe-ack for this node
      (do (upsert-probe-ack this e)
          (when (not (#{:suspect :alive} (get-status this)))
            ;; we insert neighbour from  probe ack events only if our status not #{:suspect :alive}
            (upsert-neighbour this nb)))
      (d> :probe-ack-event-probe-never-send-error (get-id this) e))))


(defmethod process-incoming-event AntiEntropy
  [^NodeObject this ^AntiEntropy e]
  (let [neighbour-id (:id e)
        nb           (get-neighbour this neighbour-id)]
    (cond
      ;; do nothing if event from unknown node
      (nil? nb)
      (d> :anti-entropy-event-unknown-neighbour-error (get-id this) e)

      ;; do nothing if event with outdated restart counter
      (not (suitable-restart-counter? this e))
      (d> :anti-entropy-event-bad-restart-counter-error (get-id this) e)

      ;; do nothing if event with outdated tx
      (not (suitable-tx? this e))
      (d> :anti-entropy-event-bad-tx-error (get-id this) e)

      ;; do not process events from not alive nodes
      (not (#{:alive :suspect} (:status nb)))
      (d> :anti-entropy-event-not-alive-neighbour-error (get-id this) e)

      :else
      (let [neighbour-vec (->> e (.-anti_entropy_data) (mapv vec->neighbour))]
        (doseq [ae-neighbour neighbour-vec]
          (if (get-neighbour this (:id ae-neighbour))
            (when (suitable-incarnation? this ae-neighbour)
              (d> :anti-entropy-event (get-id this) ae-neighbour)
              (upsert-neighbour this ae-neighbour))         ;; update existing neighbour
            (when-not (= (get-id this) (:id ae-neighbour))  ;; we don't want to put itself to a neighbours map
              (d> :anti-entropy-event (get-id this) ae-neighbour)
              (upsert-neighbour this ae-neighbour))))))))   ;; add a new neighbour


;;;;;;;;;;
;; code below is not verified

(defn- process-indirect-ack
  [this e]
  (let [nb (get-neighbour this (.-id e))]
    (upsert-neighbour this (assoc nb :tx (.-tx e) :status :alive :access :indirect))
    (delete-indirect-ping this (.-id e))
    (when (= :suspect (:status nb))
      (put-event this (new-alive-event this e))
      (inc-tx this)
      (d> :alive-event (get-id this) {:neighbour-id (.-id e) :previous-status :suspect}))
    (d> :indirect-ack-event (get-id this) e)))


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
      (if (get-indirect-ping-event this (:id e))
        (process-indirect-ack this e)
        (d> :ack-event-no-active-ping-error (get-id this) e))


      :else (do
              ;; here we work only with alive and suspect nodes
              (delete-ping this neighbour-id)
              (upsert-neighbour this (assoc nb :tx (.-tx e) :status :alive))
              (when (= :suspect (:status nb))
                (put-event this (new-alive-event this e))
                (inc-tx this)
                (d> :alive-event (get-id this) {:neighbour-id neighbour-id :previous-status :suspect}))
              (d> :ack-event (get-id this) e)))))


(defn- process-incoming-indirect-ping
  "Process indirect ping. Internal function."
  [^NodeObject this ^PingEvent e]
  (upsert-indirect-ping this e)
  (let [nb (get-neighbour this (.-neighbour_id e))]
    (new-probe-event this (:host nb) (:port nb) (keyword (str (.-id e) "->" (.-neighbour_id e))))))


(defn- process-incoming-ping
  "Process ping. Internal function."
  [^NodeObject this ^PingEvent e]
  (let [neighbour-id (:id e)
        nb           (get-neighbour this neighbour-id)]
    (cond

      ;; if this id is not equal to addressee id then do nothing
      (not= (get-id this) (.-neighbour_id e))
      (d> :ping-event-different-addressee-error (get-id this) e)

      ;; do nothing if event from unknown node
      (nil? nb)
      (d> :ping-event-unknown-neighbour-error (get-id this) e)

      ;; send dead event if outdated restart counter
      (not (suitable-restart-counter? this e))
      (let [dead-event (new-dead-event this e)]
        (d> :ping-event-bad-restart-counter-error (get-id this) e)
        ;; we don't put it to outgoing queue because it is known fact
        (send-event this dead-event neighbour-id)
        (inc-tx this)
        (d> :ping-event-reply-dead-event (get-id this) dead-event))

      ;; do nothing if event with outdated tx
      (not (suitable-tx? this e))
      (d> :ping-event-bad-tx-error (get-id this) e)

      ;; todo check host port from event and from nb map and if changed then update them in mb map

      ;; do not process events from not alive nodes
      (not (#{:alive :suspect} (:status nb)))
      (d> :ping-event-not-alive-neighbour-error (get-id this) e)

      :else
      ;; process ping event only for :alive :suspect statuses
      (let [ack-event      (new-ack-event this e)
            current-status (:status nb)]
        (inc-tx this)                                       ;; every event on node increments tx
        (d> :ping-event-ack-event (get-id this) ack-event)
        (send-event this ack-event (.-host e) (.-port e))
        ;; update tx field for neighbour, and set status as :alive
        (upsert-neighbour this (assoc nb :tx (.-tx e) :status :alive))
        (when (= :suspect current-status)
          (put-event this (new-alive-event this e))
          (inc-tx this)                                     ;; we discovered new fact that neighbour is alive
          (d> :alive-event (get-id this) {:neighbour-id neighbour-id :previous-status current-status}))))))


(defmethod process-incoming-event PingEvent
  [^NodeObject this ^PingEvent e]
  (cond
    (= :direct (.-ptype e))
    (process-incoming-ping this e)

    (= :indirect (.-ptype e))
    (process-incoming-indirect-ping this e)))



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
      (d> :incoming-udp-processor-error (get-id this) {:msg             "Bad events vector structure"
                                                       :events-vector   events-vector
                                                       :bad-udp-counter (:bad-udp-counter
                                                                          (swap! *stat update :bad-udp-counter inc))}))))


;; TODO: run periodic process for clean probe ack events - remember uuid key for non empty maps. On next iteration remembered uuids should be cleaned.

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
    (swap! (:*node this) assoc :tx 1)
    (let [{:keys [host port]} (get-value this)]
      (swap! (:*node this) assoc :*udp-server (udp/start host port (partial incoming-data-processor-fn this))))
    (when-not (s/valid? ::spec/node (get-value this))
      (throw (ex-info "Invalid node data" (->> this :*node (s/explain-data ::spec/node) spec/problems))))
    (swap! *stat assoc :bad-udp-counter 0)
    (vthread/vfuture (node-process-fn this))
    (d> :start (get-id this) {})))



(defn probe
  "Probe other node. Returns probe key
  Use probe key to catch events in `probe-events` buffer."
  ^UUID [^NodeObject this ^String neighbour-host ^long neighbour-port]
  (let [probe-event (new-probe-event this neighbour-host neighbour-port)]
    (d> :probe (get-id this) probe-event)
    (insert-probe this probe-event)
    (send-event this probe-event neighbour-host neighbour-port)
    (.-probe_key probe-event)))



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





(defn ping
  "Send Ping event to neighbour node. Also, all known messages
  Returns sent ping event if success or :ping-unknown-neighbour-id-error if error."
  [^NodeObject this neighbour-id]
  (if-let [nb (get-neighbour this neighbour-id)]
    (let [nb-host       (.-host nb)
          nb-port       (.-port nb)
          previous-ping (get-ping-event this neighbour-id)
          ping-event    (new-ping-event this neighbour-id (inc (or (:attempt-number previous-ping) 0)))
          events-vector (conj (take-events this)
                          ping-event
                          (new-anti-entropy-event this))]
      (upsert-ping this ping-event)
      (send-events this events-vector nb-host nb-port)
      ping-event)
    (do
      (d> :ping-unknown-neighbour-id-error (get-id this) {:neighbour-id neighbour-id})
      :ping-unknown-neighbour-id-error)))


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
