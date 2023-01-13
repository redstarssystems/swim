(ns org.rssys.swim
  "SWIM protocol implementation"
  (:require
    [clojure.set]
    [clojure.spec.alpha :as s]
    [cognitect.transit :as transit]
    [org.rssys.domain :as domain]
    [org.rssys.encrypt :as e]
    [org.rssys.event :as event]
    [org.rssys.spec :as spec]
    [org.rssys.udp :as udp]
    [org.rssys.vthread :refer [vthread]])
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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Common functions and constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def *config
  (atom {:enable-diag-tap?                    true          ;; Put diagnostic data to tap>
         :max-udp-size                        1432          ;; Max size of UDP packet in bytes
         :ignore-max-udp-size?                false         ;; by default, we prevent sending UDP more than :max-udp-size
         :max-payload-size                    256           ;; Max payload size in bytes
         :max-anti-entropy-items              2             ;; Max items number in anti-entropy
         :max-ping-without-ack-before-suspect 2             ;; How many pings without ack before node became suspect
         :max-ping-without-ack-before-dead    4             ;; How many pings without ack before node considered as dead


         :ping-heartbeat-ms                   500           ;; Send ping+events to neighbours every N ms
         :ack-timeout-ms                      150           ;; How much time we wait for an ack event before next ping will be sent
         :max-join-time-ms                    2000          ;; How much time node awaits join confirmation before timeout
         :rejoin-if-dead?                     true          ;; After node join, if cluster consider this node as dead, then do rejoin
         :rejoin-max-attempts                 10            ;; How many times try to rejoin
         }))


(def *stat
  (atom {;; how many UDP packet we received are bad
         :bad-udp-counter 0}))


(defn d>
  "If `*enable-diag-tap?*` is true (default), then put diagnostic data to tap>.
   Returns true if there was room in the tap> queue, false if not (dropped),
   nil if `*enable-diag-tap?*` disabled."
  [cmd-kw node-id data]
  (when (:enable-diag-tap? @*config)
    (tap> {::cmd    cmd-kw
           :ts      (System/currentTimeMillis)
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


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Domain entity builders
;;;;;;;;;;;;;;;;;;;;;;;;;


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
  "Create new NodeObject.
  `restart-counter` is an optional parameter. If omitted then it set to current milliseconds value.
  Returns new NodeObject instance."

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
                      :restart-counter      (or restart-counter (System/currentTimeMillis))
                      :tx                   0
                      :ping-events          {}               ;; active direct pings
                      :indirect-ping-events {}               ;; active indirect pings
                      :payload              {}               ;; data that this node claims in cluster about itself
                      :*udp-server          nil
                      :outgoing-events      []               ;; outgoing events that we'll send to random logN neighbours next time
                      :ping-round-buffer    []               ;; we take logN neighbour ids to send events from event queue
                      :probe-events         {}               ;; outgoing probe events
                      })))


;;;;;;;;;;;;;;;;;;;;;;
;; NodeObject Getters
;;;;;;;;;;;;;;;;;;;;;;


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


(defn neighbour-exist?
  "Returns true if neighbour exist, otherwise false"
  ^Boolean
  [^NodeObject this ^UUID id]
  (boolean (get-neighbour this id)))


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
  "Get active indirect ping event by neighbour id if exist"
  ^IndirectPingEvent
  [^NodeObject this ^UUID neighbour-id]
  (get (get-indirect-ping-events this) neighbour-id))


(defn get-payload
  "Get node payload"
  [^NodeObject this]
  (:payload (get-value this)))


(defn get-outgoing-events
  "Get vector of prepared outgoing events"
  [^NodeObject this]
  (.-outgoing_events (get-value this)))


(defn get-ping-round-buffer
  "Returns vector of ids intended for pings."
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


;;;;;;;;;;;;;;;;;;;;;
;; NodeObject Setters
;;;;;;;;;;;;;;;;;;;;;


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
  (d> :set-status (get-id this) {:old-status (get-status this)
                                 :new-status new-status})
  (swap! (:*node this) assoc :status new-status))


(defn set-dead-status
  [^NodeObject this]
  (set-status this :dead))


(defn set-stop-status
  [^NodeObject this]
  (set-status this :stop))


(defn set-left-status
  [^NodeObject this]
  (set-status this :left))


(defn set-join-status
  [^NodeObject this]
  (set-status this :join))


(defn set-alive-status
  [^NodeObject this]
  (set-status this :alive))


(defn set-suspect-status
  [^NodeObject this]
  (set-status this :suspect))


(defn set-ping-round-buffer
  "Set vector of ids intended for pings."
  [^NodeObject this new-round-buffer]
  (swap! (:*node this) assoc :ping-round-buffer new-round-buffer))


(defn set-payload
  "Set new payload for this node.
  Max size of payload is limited by `*max-payload-size*`."
  [^NodeObject this payload & {:keys [max-payload-size] :or {max-payload-size (:max-payload-size @*config)}}]
  (let [actual-size (alength ^bytes (serialize payload))]
    (when (> actual-size max-payload-size)
      (throw (ex-info "Payload size is too big" {:max-allowed max-payload-size :actual-size actual-size}))))
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


(defn set-tx
  "Set node tx"
  [^NodeObject this ^long tx]
  (when-not (s/valid? ::spec/tx tx)
    (throw (ex-info "Invalid tx data"
             (->> tx (s/explain-data ::spec/tx) spec/problems))))
  (d> :set-tx (get-id this) {:tx tx})
  (swap! (:*node this) assoc :tx tx))


(defn inc-tx
  "Increment node tx"
  [^NodeObject this]
  (swap! (:*node this) assoc :tx (inc (get-tx this))))



(defn upsert-neighbour
  "Update existing or insert new neighbour to neighbours map.
  Do nothing if `this` id equals to `neighbour-node` id.
  Returns void."
  [^NodeObject this neighbour-node]
  (when-not (s/valid? ::spec/neighbour-node neighbour-node)
    (throw (ex-info "Invalid neighbour node data"
             (->> neighbour-node (s/explain-data ::spec/neighbour-node) spec/problems))))
  (let [neighbour-not-exist? (not (boolean (get-neighbour this (:id neighbour-node))))]
    (when (and neighbour-not-exist? (cluster-size-exceed? this))
      (d> :upsert-neighbour-cluster-size-exceeded-error (get-id this)
        {:nodes-in-cluster (nodes-in-cluster this)
         :cluster-size     (get-cluster-size this)})
      (throw (ex-info "Cluster size exceeded" {:nodes-in-cluster (nodes-in-cluster this)
                                               :cluster-size     (get-cluster-size this)}))))
  (when-not (= (get-id this) (:id neighbour-node))
    (d> :upsert-neighbour (get-id this) {:neighbour-node neighbour-node})
    (swap! (:*node this) assoc :neighbours (assoc
                                             (get-neighbours this)
                                             (:id neighbour-node)
                                             (assoc neighbour-node :updated-at (System/currentTimeMillis))))))


(defn delete-neighbour
  "Delete neighbour from neighbours map"
  [^NodeObject this ^UUID neighbour-id]
  (d> :delete-neighbour (get-id this) {:neighbour-id neighbour-id})
  (swap! (:*node this) assoc :neighbours (dissoc (get-neighbours this) neighbour-id)))


(defn delete-neighbours
  "Delete all neighbours from neighbours map.
  Returns number of deleted neighbours."
  [^NodeObject this]
  (let [nb-count (count (get-neighbours this))]
    (swap! (:*node this) assoc :neighbours {})
    (d> :delete-neighbours (get-id this) {:deleted-num nb-count})
    nb-count))


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


(defn insert-ping
  "Insert new active ping event in a map.
  [neighbour-id ts] is used as a key in ping events map.
  Returns ping-id - [neighbour-id ts] as key of ping event in a map"
  [^NodeObject this ^PingEvent ping-event]
  (when-not (s/valid? ::spec/ping-event ping-event)
    (throw (ex-info "Invalid ping event data" (->> ping-event (s/explain-data ::spec/ping-event) spec/problems))))

  (let [ping-id            [(.-neighbour_id ping-event) (.-ts ping-event)]]
    (d> :insert-ping (get-id this) {:ping-event ping-event})
    (swap! (:*node this) assoc :ping-events (assoc (get-ping-events this) ping-id ping-event))
    ping-id))


(defn delete-ping
  "Delete active ping event from map"
  [^NodeObject this ping-id]
  (d> :delete-ping (get-id this) {:neighbour-id (first ping-id) :ts (second ping-id)})
  (swap! (:*node this) assoc :ping-events (dissoc (get-ping-events this) ping-id)))


(defn upsert-indirect-ping
  "Update existing or insert new active indirect ping event in map.
  `neighbour-id` is used as a key in indirect ping events map.
  Returns key (`neighbour-id`) of indirect ping event in a map"
  [^NodeObject this ^IndirectPingEvent indirect-ping-event]
  (when-not (s/valid? ::spec/indirect-ping-event indirect-ping-event)
    (throw (ex-info "Invalid indirect ping event data"
             (->> indirect-ping-event (s/explain-data ::spec/indirect-ping-event) spec/problems))))
  (let [indirect-key            (.-neighbour_id indirect-ping-event)
        previous-indirect-ping (get-indirect-ping-event this indirect-key)
        new-attempt-number     (if previous-indirect-ping
                                 (inc (.-attempt_number previous-indirect-ping))
                                 (.-attempt_number indirect-ping-event))
        indirect-ping-event'   (assoc indirect-ping-event :attempt-number new-attempt-number)]
    (d> :upsert-indirect-ping (get-id this) {:indirect-ping-event indirect-ping-event'
                                             :indirect-id         indirect-key})
    (swap! (:*node this) assoc :indirect-ping-events
      (assoc (get-indirect-ping-events this) indirect-key indirect-ping-event'))
    indirect-key))


(defn delete-indirect-ping
  "Delete active ping event from map"
  [^NodeObject this neighbour-id]
  (d> :delete-indirect-ping (get-id this) {:indirect-id neighbour-id})
  (swap! (:*node this) assoc :indirect-ping-events (dissoc (get-indirect-ping-events this) neighbour-id)))


(defn insert-probe
  "Insert probe key from probe event in a probe events map {probe-key nil}"
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
  `:probe-key` from event is used as a key in probe-events map."
  [^NodeObject this ^ProbeAckEvent probe-ack-event]
  (when-not (s/valid? ::spec/probe-ack-event probe-ack-event)
    (throw (ex-info "Invalid probe ack event data" (->> probe-ack-event (s/explain-data ::spec/probe-ack-event) spec/problems))))
  (d> :upsert-probe-ack (get-id this) {:probe-ack-event probe-ack-event})
  (swap! (:*node this) assoc :probe-events (assoc (get-probe-events this) (.-probe_key probe-ack-event) probe-ack-event))
  (.-probe_key probe-ack-event))


;;;;;;;;;;;;;;;;;;
;; Event builders
;;;;;;;;;;;;;;;;;;


(defn new-probe-event
  "Returns new probe event. Increase tx of `this` node."
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
      (throw (ex-info "Invalid probe data" (spec/problems (s/explain-data ::spec/probe-event probe-event))))
      probe-event)))


(defn new-probe-ack-event
  "Returns new probe ack event. Increase tx of `this` node."
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
      (throw (ex-info "Invalid probe ack data" (spec/problems (s/explain-data ::spec/probe-ack-event ack-event))))
      ack-event)))


(defn new-ping-event
  "Returns new ping event. Increase tx of `this` node."
  ^PingEvent [^NodeObject this ^UUID neighbour-id attempt-number]
  (let [ping-event (event/map->PingEvent {:cmd-type        (:ping event/code)
                                          :id              (get-id this)
                                          :host            (get-host this)
                                          :port            (get-port this)
                                          :restart-counter (get-restart-counter this)
                                          :tx              (get-tx this)
                                          :neighbour-id    neighbour-id
                                          :attempt-number  attempt-number
                                          :ts              (System/currentTimeMillis)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/ping-event ping-event)
      (throw (ex-info "Invalid ping data" (spec/problems (s/explain-data ::spec/ping-event ping-event))))
      ping-event)))


(defn new-ack-event
  "Returns new Ack event.
  Increase tx of `this` node."
  ^AckEvent [^NodeObject this ^ISwimEvent e]
  (let [ack-event (event/map->AckEvent {:cmd-type        (:ack event/code)
                                        :id              (get-id this)
                                        :restart-counter (get-restart-counter this)
                                        :tx              (get-tx this)
                                        :neighbour-id    (:id e)
                                        :neighbour-tx    (:tx e)
                                        :attempt-number  (:attempt-number e)
                                        :ts              (:ts e)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/ack-event ack-event)
      (throw (ex-info "Invalid ack data" (spec/problems (s/explain-data ::spec/ack-event ack-event))))
      ack-event)))



(defn new-indirect-ping-event
  "Returns new indirect ping event. Increase tx of `this` node."
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
                                           :attempt-number    attempt-number
                                           :ts                (System/currentTimeMillis)})]
        (inc-tx this)
        (if-not (s/valid? ::spec/indirect-ping-event indirect-ping-event)
          (throw (ex-info "Invalid indirect ping data"
                   (spec/problems (s/explain-data ::spec/indirect-ping-event indirect-ping-event))))
          indirect-ping-event)))))



(defn new-indirect-ack-event
  "Returns new indirect ack event. Increase tx of `this` node."
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
      (throw (ex-info "Invalid indirect ack data"
               (spec/problems (s/explain-data ::spec/indirect-ack-event indirect-ack-event))))
      indirect-ack-event)))



(defn new-alive-event
  "Returns new Alive event. Increase tx of `this` node."
  ^AliveEvent [^NodeObject this ^ISwimEvent e]
  (let [nb (get-neighbour this (:id e))
        alive-event
        (event/map->AliveEvent {:cmd-type                  (:alive event/code)
                                :id                        (get-id this)
                                :restart-counter           (get-restart-counter this)
                                :tx                        (get-tx this)
                                :neighbour-id              (:id e)
                                :neighbour-restart-counter (:restart-counter e)
                                :neighbour-tx              (:tx e)
                                :neighbour-host            (:host nb)
                                :neighbour-port            (:port nb)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/alive-event alive-event)
      (throw (ex-info "Invalid alive data"
               (spec/problems (s/explain-data ::spec/alive-event alive-event))))
      alive-event)))



(defn new-dead-event
  "Returns new dead event. Increase tx of `this` node."
  (^DeadEvent [^NodeObject this ^UUID neighbour-id]
    (let [nb                 (get-neighbour this neighbour-id)
          nb-restart-counter (:restart-counter nb)
          nb-tx              (:tx nb)]
      (new-dead-event this neighbour-id nb-restart-counter nb-tx)))

  (^DeadEvent [^NodeObject this neighbour-id neighbour-restart-counter neighbour-tx]
    (let [dead-event
          (event/map->DeadEvent {:cmd-type                  (:dead event/code)
                                 :id                        (get-id this)
                                 :restart-counter           (get-restart-counter this)
                                 :tx                        (get-tx this)
                                 :neighbour-id              neighbour-id
                                 :neighbour-restart-counter neighbour-restart-counter
                                 :neighbour-tx              neighbour-tx})]
      (inc-tx this)
      (if-not (s/valid? ::spec/dead-event dead-event)
        (throw (ex-info "Invalid dead event data"
                 (spec/problems (s/explain-data ::spec/dead-event dead-event))))
        dead-event))))



(defn neighbour->vec
  "Convert NeighbourNode to vector of values.
  Field :updated-at is omitted.
  Returns vector of values"
  [^NeighbourNode nb]
  (when (instance? NeighbourNode nb)
    [(:id nb)
     (:host nb)
     (:port nb)
     ((:status nb) event/code)
     (if (= :direct (:access nb)) 0 1)
     (:restart-counter nb)
     (:tx nb)
     (:payload nb)]))


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
    (throw (ex-info "Invalid data in vector for NeighbourNode" {:v v}))))


(defn build-anti-entropy-data
  "Build anti-entropy data – subset of known nodes from neighbours map.
  This data is propagated from node to node and thus nodes can get knowledge about unknown nodes.
  To apply anti-entropy data receiver should compare incarnation pair [restart-counter tx] and apply only
  if node has older data.
  Returns vector of known neighbors size of `num` if any or empty vector.
  Any item in returned vector is vectorized NeighbourNode.
  If key :neighbour-id present then returns anti-entropy data for this neighbour only"
  [^NodeObject this & {:keys [num neighbour-id] :or {num (:max-anti-entropy-items @*config)}}]
  (if neighbour-id
    (if-let [ae-data (neighbour->vec (get-neighbour this neighbour-id))]
      [ae-data]
      [])
    (or
      (some->>
        (get-neighbours this)
        vals
        shuffle
        (take num)
        (map neighbour->vec)
        vec)
      [])))


(defn new-anti-entropy-event
  "Returns anti-entropy event. Increase tx of `this` node.
  If key `:neighbour-id` present in `ks` then returns anti-entropy data for this neighbour only.
  If key `:num` present in `ks` then returns anti-entropy data for given number of neighbours.
  Default value for `:num` in `ks` is :max-anti-entropy-items"
  ^AntiEntropy [^NodeObject this & {:keys [] :as ks}]
  (let [anti-entropy-data (build-anti-entropy-data this ks)
        ae-event          (event/map->AntiEntropy {:cmd-type          (:anti-entropy event/code)
                                                   :id                (get-id this)
                                                   :restart-counter   (get-restart-counter this)
                                                   :tx                (get-tx this)
                                                   :anti-entropy-data anti-entropy-data})]
    (inc-tx this)
    (if-not (s/valid? ::spec/anti-entropy-event ae-event)
      (throw (ex-info "Invalid anti-entropy event" (spec/problems (s/explain-data ::spec/anti-entropy-event ae-event))))
      ae-event)))



(defn new-cluster-size-event
  "Returns new NewClusterSizeEvent event. Increase tx of `this` node.
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
      (throw (ex-info "Invalid cluster size data" (spec/problems (s/explain-data ::spec/new-cluster-size-event ncs-event))))
      ncs-event)))



(defn new-join-event
  "Returns new JoinEvent event. Increase tx of `this` node."
  ^JoinEvent [^NodeObject this]
  (let [join-event (event/map->JoinEvent {:cmd-type        (:join event/code)
                                          :id              (get-id this)
                                          :restart-counter (get-restart-counter this)
                                          :tx              (get-tx this)
                                          :host            (get-host this)
                                          :port            (get-port this)})]
    (inc-tx this)
    (if-not (s/valid? ::spec/join-event join-event)
      (throw (ex-info "Invalid join event data"
               (spec/problems (s/explain-data ::spec/join-event join-event))))
      join-event)))




(defn new-suspect-event
  "Returns new suspect event. Increase tx of `this` node."
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




(defn new-left-event
  "Returns new LeftEvent event. Increase tx of `this` node."
  ^LeftEvent [^NodeObject this]
  (let [e (event/map->LeftEvent {:cmd-type        (:left event/code)
                                 :id              (get-id this)
                                 :restart-counter (get-restart-counter this)
                                 :tx              (get-tx this)})]
    (inc-tx this)
    e))



(defn new-payload-event
  "Returns new PayloadEvent event. Increase tx of `this` node."
  ^PayloadEvent [^NodeObject this]
  (let [e (event/map->PayloadEvent {:cmd-type        (:payload event/code)
                                    :id              (get-id this)
                                    :restart-counter (get-restart-counter this)
                                    :tx              (get-tx this)
                                    :payload         (get-payload this)})]
    (inc-tx this)
    e))


;;;;;;;;;;;;;;;;;;;
;; Helper functions
;;;;;;;;;;;;;;;;;;;

(defn get-nb-payload
  "Get neighbour payload. If neighbour not exist returns nil.
  Returns map."
  [^NodeObject this ^UUID neighbour-id]
  (when-let [nb (get-neighbour this neighbour-id)]
    (.-payload nb)))


(defn suitable-restart-counter?
  "Check restart counter is suitable from event or given neighbour.
  It should be greater or equal than restart counter this node knows.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this event-or-neighbour]
  (let [neighbour-id (:id event-or-neighbour)]
    (when-let [nb ^NeighbourNode (get-neighbour this neighbour-id)]
      (>= (:restart-counter event-or-neighbour) (.-restart_counter nb)))))


(defn suitable-tx?
  "Check tx is suitable from event or given neighbour.
  It should be greater than tx this node knows about neighbour.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this event-or-neighbour]
  (let [neighbour-id (:id event-or-neighbour)]
    (when-let [nb ^NeighbourNode (get-neighbour this neighbour-id)]
      (> (:tx event-or-neighbour) (.-tx nb)))))


(defn suitable-incarnation?
  "Check incarnation [restart-counter tx] is suitable from event or given neighbour.
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
  "Returns the oldest neighbour  with particular status by :updated-at attribute.
  If `status-set` parameter is omitted then use all statuses"
  ([^NodeObject this]
    (get-oldest-neighbour this spec/status-set))
  ([^NodeObject this ^PersistentHashSet status-set]
    (let [desired-nb (get-neighbours-with-status this status-set)]
      (->> desired-nb (sort-by :updated-at) first))))


(def alive-status-set #{:alive :suspect})


(defn alive-neighbour?
  "Returns true if neighbour has alive statuses."
  [^NeighbourNode nb]
  (boolean (alive-status-set (:status nb))))


(defn alive-node?
  "Returns true if given node has alive statuses."
  [^NodeObject this]
  (boolean (alive-status-set (get-status this))))


(defn set-nb-tx
  "Update :tx field for neighbour in neighbours map.
  If new tx value is less or equals to current :tx value then do nothing.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id ^long new-tx]
  (when-let [nb (get-neighbour this neighbour-id)]
    (when (> new-tx (.-tx nb))
      (upsert-neighbour this (assoc nb :tx new-tx)))))


(defn set-nb-restart-counter
  "Set :restart-counter field for neighbour in neighbours map.
  If new restart counter value is less or equals to current :restart-counter value then do nothing.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id ^long new-restart-counter]
  (when-let [nb (get-neighbour this neighbour-id)]
    (when (> new-restart-counter (.-restart_counter nb))
      (upsert-neighbour this (assoc nb :restart-counter new-restart-counter)))))


(defn set-nb-status
  "Set :status field for neighbour in neighbours map.
  Status should be one of: #{:stop :join :alive :suspect :left :dead :unknown}
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id ^Keyword status]
  (when-let [nb (get-neighbour this neighbour-id)]
    (upsert-neighbour this (assoc nb :status status))))


(defn set-nb-dead-status
  [^NodeObject this ^UUID neighbour-id]
  (d> :set-neighbour-dead-status (get-id this) {:neighbour-id neighbour-id})
  (set-nb-status this neighbour-id :dead))


(defn set-nb-left-status
  [^NodeObject this ^UUID neighbour-id]
  (d> :set-neighbour-left-status (get-id this) {:neighbour-id neighbour-id})
  (set-nb-status this neighbour-id :left))


(defn set-nb-alive-status
  [^NodeObject this ^UUID neighbour-id]
  (d> :set-neighbour-alive-status (get-id this) {:neighbour-id neighbour-id})
  (set-nb-status this neighbour-id :alive))


(defn set-nb-suspect-status
  [^NodeObject this ^UUID neighbour-id]
  (d> :set-neighbour-suspect-status (get-id this) {:neighbour-id neighbour-id})
  (set-nb-status this neighbour-id :suspect))


(defn set-nb-direct-access
  "Set :direct access for neighbour in neighbours map.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id]
  (when-let [nb (get-neighbour this neighbour-id)]
    (upsert-neighbour this (assoc nb :access :direct))))


(defn set-nb-indirect-access
  "Set :indirect access for neighbour in neighbours map.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id]
  (when-let [nb (get-neighbour this neighbour-id)]
    (upsert-neighbour this (assoc nb :access :indirect))))


(defn set-nb-payload
  "Set payload for neighbour in neighbours map.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id payload &
   {:keys [max-payload-size] :or {max-payload-size (:max-payload-size @*config)}}]
  (let [actual-size (alength ^bytes (serialize payload))]
    (when (> actual-size max-payload-size)
      (d> :set-nb-payload-too-big-error (get-id this)
        {:max-allowed max-payload-size :actual-size actual-size :neighbour-id neighbour-id})
      (throw (ex-info "Payload size for neighbour is too big"
               {:max-allowed max-payload-size :actual-size actual-size :neighbour-id neighbour-id}))))
  (when-let [nb (get-neighbour this neighbour-id)]
    (upsert-neighbour this (assoc nb :payload payload))))


(defn- random-order-xs
  "Takes coll and returns lazy sequence of coll elements in random order"
  [xs]
  (lazy-cat (shuffle xs) (random-order-xs xs)))


(defn- remove-not-alive-nodes!
  "Remove left or dead node ids from round buffer."
  [^NodeObject this alive-ids-coll]
  (set-ping-round-buffer this  (filterv #(contains? (into #{} alive-ids-coll) %) (get-ping-round-buffer this))))


(defn- add-ids-for-next-round!
  "Add node ids in random order to buffer for next round,
  if number of elements in round buffer less than required n"
  [^NodeObject this n alive-ids-coll]
  (when (> n (count (get-ping-round-buffer this)))
    (let [required-number (* (count alive-ids-coll) (inc (or (safe (quot n (count alive-ids-coll))) 1)))
          new-xs (take  required-number (random-order-xs alive-ids-coll))]
      (set-ping-round-buffer this (vec (concat (get-ping-round-buffer this) new-xs))))))


(defn- take-random-ids!
  "Returns n ids from round buffer. Taken elements will be removed from round buffer."
  [^NodeObject this n]
  (let [buffer (get-ping-round-buffer this)
        result (take n buffer)]
    (set-ping-round-buffer this (vec (drop n buffer)))
    result))


(defn take-ids-for-ping
  "Returns vector of node ids for ping. Returned number of ids is always <= than alive nodes.
  `n` - required number of ids.
  An idea behind this function that we need to achieve a uniform distribution of ping events inside cluster in one round.
  It guarantees that every node in cluster receive an equal number of pings and there will be no ping starvation."
  [^NodeObject this n]
  (let [alive-ids-coll (mapv :id (get-alive-neighbours this))]
    (if (seq alive-ids-coll)
      (do (remove-not-alive-nodes! this alive-ids-coll)
          (add-ids-for-next-round! this n alive-ids-coll)
          (vec (distinct (take-random-ids! this n))))
      [])))


;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Event sending functions
;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn send-events
  "Send events to host:port. All events will be serialized and encrypted.
   Returns size of sent data in bytes. "
  [^NodeObject this events-vector neighbour-host neighbour-port &
   {:keys [max-udp-size ignore-max-udp-size?]
    :or   {max-udp-size         (:max-udp-size @*config)
           ignore-max-udp-size? (:ignore-max-udp-size? @*config)}}]
  (let [secret-key             (-> this get-cluster :secret-key)
        prepared-events-vector (mapv #(.prepare ^ISwimEvent %) events-vector)
        data                   ^bytes (e/encrypt-data secret-key (serialize prepared-events-vector))]
    (when (> (alength data) max-udp-size)
      (d> :send-events-too-big-udp-error (get-id this) {:udp-size (alength data)})
      (when-not ignore-max-udp-size?
        (throw (ex-info "UDP packet is too big" {:max-allowed (:max-udp-size @*config)}))))
    (d> :send-events-udp-size (get-id this) {:udp-size (alength data)})
    (udp/send-packet data neighbour-host neighbour-port)))


(defn send-event-by-host
  "Send one event to a neighbour using its host and port.
  Returns size of sent data in bytes."
  [^NodeObject this ^ISwimEvent event neighbour-host neighbour-port & {:keys [attach-anti-entropy?] :as opts}]
  (if attach-anti-entropy?
    (send-events this [event (new-anti-entropy-event this)] neighbour-host neighbour-port opts)
    (send-events this [event] neighbour-host neighbour-port opts)))


(defn send-event-by-id
  "Send one event to a neighbour using its id.
  Returns size of sent data in bytes."
  [^NodeObject this ^ISwimEvent event ^UUID neighbour-id & {:keys [] :as opts}]
  (if-let [nb (get-neighbour this neighbour-id)]
    (let [nb-host (.-host nb)
          nb-port (.-port nb)]
      (send-event-by-host this event nb-host nb-port opts))
    (do
      (d> :send-event-by-id-unknown-neighbour-id-error (get-id this) {:neighbour-id neighbour-id})
      (throw (ex-info "Unknown neighbour id" {:neighbour-id neighbour-id})))))


(defn send-event
  "Send one event to a neighbour.
  Returns size of sent data in bytes."
  ([^NodeObject this ^ISwimEvent event neighbour-host neighbour-port]
    (send-event-by-host this event neighbour-host neighbour-port {:attach-anti-entropy? false}))
  ([^NodeObject this ^ISwimEvent event ^UUID neighbour-id]
    (send-event-by-id this event neighbour-id {:attach-anti-entropy? false})))


(defn send-event-ae
  "Send one event + anti-entropy data to a neighbour.
  Returns size of sent data in bytes."
  ([^NodeObject this ^ISwimEvent event neighbour-host neighbour-port]
    (send-event-by-host this event neighbour-host neighbour-port {:attach-anti-entropy? true}))
  ([^NodeObject this ^ISwimEvent event ^UUID neighbour-id]
    (send-event-by-id this event neighbour-id {:attach-anti-entropy? true})))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Event processing functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


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


(defmulti event-processing (fn [_ e] (type e)))


(defmethod event-processing ProbeEvent
  [^NodeObject this ^ProbeEvent e]
  (let [probe-ack-event (new-probe-ack-event this e)]
    (d> :probe-ack-event (get-id this) probe-ack-event)
    (send-event this probe-ack-event (.-host e) (.-port e))))


(defn- expected-probe-event?
  "Returns true if probe key present in probe events map and ProbeAckEvent contains id of this node, otherwise false."
  [^NodeObject this ^ProbeAckEvent e]
  (and (contains? (get-probe-events this) (.-probe_key e))  ;; check  we send probe-event before
    (= (.-neighbour_id e) (get-id this))))                  ;; this probe-ack event is intended for this node



(defmethod event-processing ProbeAckEvent
  [^NodeObject this ^ProbeAckEvent e]
  (let [nb
        (new-neighbour-node {:id              (.-id e)
                             :host            (.-host e)
                             :port            (.-port e)
                             :status          (.-status e)
                             :access          :direct
                             :restart-counter (.-restart_counter e)
                             :tx              (.-tx e)
                             :payload         {}
                             :updated-at      (System/currentTimeMillis)})]

    (if (expected-probe-event? this e)
      (do (upsert-probe-ack this e)
          (when (not (alive-node? this))
            (if (not (cluster-size-exceed? this))
              (upsert-neighbour this nb)
              (d> :probe-ack-event-cluster-size-exceeded-error (get-id this) {:nodes-in-cluster (nodes-in-cluster this)
                                                                              :cluster-size     (get-cluster-size this)})))
          (d> :probe-ack-event (get-id this) nb))
      (d> :probe-ack-event-probe-never-send-error (get-id this) e))))



(defmethod event-processing AntiEntropy
  [^NodeObject this ^AntiEntropy e]
  (let [sender-id (:id e)
        sender    (or (get-neighbour this sender-id) :unknown-neighbour)]
    (cond

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :anti-entropy-event-unknown-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :anti-entropy-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :anti-entropy-event-bad-tx-error (get-id this) e)

      (not (alive-neighbour? sender))
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
              (upsert-neighbour this ae-neighbour))))       ;; add a new neighbour
        (upsert-neighbour this (assoc sender :tx (.-tx e) :restart-counter (.-restart_counter e)))))))



(defn- expected-indirect-ack-event?
  "Returns true if we sent indirect-ping-event before, otherwise false"
  [^NodeObject this ^IndirectAckEvent e]
  (let [indirect-ping-request-exist? (boolean (get-indirect-ping-event this (:id e)))
        receiver-this-node?          (= (.-neighbour_id e) (get-id this))]
    (and indirect-ping-request-exist? receiver-this-node?)))


(defn- expected-ack-event?
  "Returns true if ack event corresponds to ping-event from this node,
   otherwise false"
  [^NodeObject this ^AckEvent e]
  (let [ping-request-exist? (boolean (get-ping-event this [(.-id e) (.-ts e)]))
        receiver-this-node? (= (.-neighbour_id e) (get-id this))]
    (and ping-request-exist? receiver-this-node?)))


(defn- intermediate-node?
  "Returns true if this node is intermediate for indirect ping/ack event, otherwise false"
  [^NodeObject this e]
  (and
    (= (get-host this) (:intermediate-host e))
    (= (get-port this) (:intermediate-port e))
    (= (get-id this) (:intermediate-id e))))


(defmethod event-processing IndirectAckEvent
  [^NodeObject this ^IndirectAckEvent e]
  (let [sender-id (:id e)
        sender    (or (get-neighbour this sender-id) :unknown-neighbour)]

    (cond

      (not (alive-node? this))
      (d> :indirect-ack-event-not-alive-node-error (get-id this) e)

      (intermediate-node? this e)
      (do
        (d> :intermediate-node-indirect-ack-event (get-id this) e)
        (send-event this e (.-neighbour_host e) (.-neighbour_port e)))

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :indirect-ack-event-unknown-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :indirect-ack-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :indirect-ack-event-bad-tx-error (get-id this) e)

      (not (expected-indirect-ack-event? this e))
      (d> :indirect-ack-event-not-expected-error (get-id this) e)

      :else
      (do
        (upsert-neighbour this (assoc sender :tx (.-tx e) :access :indirect :restart-counter (.-restart_counter e)
                                 :status :alive))
        (d> :indirect-ack-event (get-id this) e)
        (delete-indirect-ping this sender-id)))))


(defmethod event-processing IndirectPingEvent
  [^NodeObject this ^IndirectPingEvent e]
  (let [sender-id (:id e)
        sender    (or (get-neighbour this sender-id) :unknown-neighbour)]

    (cond

      (not (alive-node? this))
      (d> :indirect-ping-event-not-alive-node-error (get-id this) e)

      (intermediate-node? this e)
      (do
        (d> :intermediate-node-indirect-ping-event (get-id this) e)
        (send-event this e (.-neighbour_host e) (.-neighbour_port e)))

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :indirect-ping-event-unknown-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :indirect-ping-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :indirect-ping-event-bad-tx-error (get-id this) e)

      (not (= (get-id this) (.-neighbour_id e)))
      (d> :indirect-ping-event-neighbour-id-mismatch-error (get-id this) e)

      :else
      (let [_                  (d> :indirect-ping-event (get-id this) e)
            _                  (upsert-neighbour this (assoc sender :tx (.-tx e) :restart-counter (.-restart_counter e)))
            indirect-ack-event (new-indirect-ack-event this e)]
        (d> :indirect-ack-event (get-id this) indirect-ack-event)
        (send-event this indirect-ack-event (.-intermediate_host e) (.-intermediate_port e))))))


(defmethod event-processing AckEvent
  [^NodeObject this ^AckEvent e]
  (let [sender-id (:id e)
        sender    (or (get-neighbour this sender-id) :unknown-neighbour)]
    (cond

      (not (alive-node? this))
      (d> :ack-event-not-alive-node-error (get-id this) e)

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :ack-event-unknown-neighbour-error (get-id this) e)

      (not (alive-neighbour? sender))
      (d> :ack-event-not-alive-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :ack-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :ack-event-bad-tx-error (get-id this) e)

      (not (expected-ack-event? this e))
      (d> :ack-event-not-expected-error (get-id this) e)

      :else
      (do
        (d> :ack-event (get-id this) e)
        (delete-ping this [sender-id (.-ts e)])
        (upsert-neighbour this (assoc sender :tx (.-tx e) :restart-counter (.-restart_counter e) :status :alive))))))


(defmethod event-processing PingEvent
  [^NodeObject this ^PingEvent e]
  (let [sender-id (:id e)
        sender       (or (get-neighbour this sender-id) :unknown-neighbour)]

    (cond

      (not (alive-node? this))
      (d> :ping-event-not-alive-node-error (get-id this) e)

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :ping-event-unknown-neighbour-error (get-id this) e)

      (not (alive-neighbour? sender))
      (do
        (send-event-ae this (new-dead-event this (.-id e) (.-restart_counter e) (.-tx e)) (.-host e) (.-port e))
        (d> :ping-event-not-alive-neighbour-error (get-id this) e))

      (not (suitable-restart-counter? this e))
      (do
        (send-event-ae this (new-dead-event this (.-id e) (.-restart_counter e) (.-tx e)) (.-host e) (.-port e))
        (d> :ping-event-bad-restart-counter-error (get-id this) e))

      (not (suitable-tx? this e))
      (d> :ping-event-bad-tx-error (get-id this) e)

      (not (= (get-id this) (.-neighbour_id e)))
      (d> :ping-event-neighbour-id-mismatch-error (get-id this) e)

      :else
      (do
        (d> :ping-event (get-id this) e)
        (upsert-neighbour this (assoc sender :host (.-host e) :port (.-port e) :tx (.-tx e)
                                 :restart-counter (.-restart_counter e) :status :alive))
        (let [ack-event (new-ack-event this e)]
          (send-event-ae this ack-event sender-id)
          (d> :ack-event (get-id this) ack-event))))))


(defmethod event-processing JoinEvent
  [^NodeObject this ^JoinEvent e]
  (cond

    (not (alive-node? this))
    (d> :join-event-not-alive-node-error (get-id this) e)

    (and (neighbour-exist? this (.-id e))
      (not (suitable-restart-counter? this e)))
    (do
      (send-event-ae this (new-dead-event this (.-id e) (.-restart_counter e) (.-tx e)) (.-host e) (.-port e))
      (d> :join-event-bad-restart-counter-error (get-id this) e))

    (and
      (not (neighbour-exist? this (.-id e)))
      (cluster-size-exceed? this))
    (do
      (send-event-ae this (new-dead-event this (.-id e) (.-restart_counter e) (.-tx e)) (.-host e) (.-port e))
      (d> :join-event-cluster-size-exceeded-error (get-id this) e))

    (and (neighbour-exist? this (.-id e))
      (not (suitable-tx? this e))
      (= (.-restart_counter e) (:restart-counter (get-neighbour this (.-id e)))))
    (d> :join-event-bad-tx-error (get-id this) e)

    :else
    (let [_           (d> :join-event (get-id this) e)
          nb          (new-neighbour-node (.-id e) (.-host e) (.-port e))
          _           (upsert-neighbour this (assoc nb :tx (.-tx e) :restart-counter (.-restart_counter e)
                                               :status :alive :access :direct))
          alive-event (new-alive-event this e)
          cluster-size-event (new-cluster-size-event this (get-cluster-size this))
          ae-event (new-anti-entropy-event this)]
      (send-events this [alive-event cluster-size-event ae-event] (.-host e) (.-port e))
      (put-event this alive-event))))


(defn- alive-event-join-confirm?
  "Check that status of `this` is join and alive event about `this` node."
  [^NodeObject this ^AliveEvent e]
  (and (= (.-neighbour_id e) (get-id this))
    (= :join (get-status this))))


(defmethod event-processing AliveEvent
  [^NodeObject this ^AliveEvent e]
  (let [sender-id (.-id e)
        sender (or (get-neighbour this (:id e)) :unknown-neighbour)]

    (cond

      (alive-event-join-confirm? this e)
      (do
        (set-status this :alive)
        (upsert-neighbour this (assoc sender :tx (.-tx e) :restart-counter (.-restart_counter e)))
        (d> :alive-event-join-confirmed (get-id this) e))

      (not (alive-node? this))
      (d> :alive-event-not-alive-node-error (get-id this) e)

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :alive-event-unknown-neighbour-error (get-id this) e)

      (not (alive-neighbour? sender))
      (d> :alive-event-not-alive-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :alive-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :alive-event-bad-tx-error (get-id this) e)

      :else
      (let [_        (d> :alive-event (get-id this) e)
            alive-id (.-neighbour_id e)
            alive-nb (assoc (new-neighbour-node alive-id (.-neighbour_host e) (.-neighbour_port e))
                       :tx (.-neighbour_tx e)
                       :restart-counter (.-neighbour_restart_counter e))]

        (upsert-neighbour this (assoc sender :tx (.-tx e) :restart-counter (.-restart_counter e)))

        (when (or
                (not (neighbour-exist? this alive-id))
                (suitable-incarnation? this alive-nb))
          (upsert-neighbour this alive-nb)
          (put-event this e))))))



(defmethod event-processing NewClusterSizeEvent
  [^NodeObject this ^NewClusterSizeEvent e]
  (let [sender-id (:id e)
        sender    (or (get-neighbour this sender-id) :unknown-neighbour)
        new-size (.-new_cluster_size e)
        alive-number (nodes-in-cluster this)]
    (cond

      (not (alive-node? this))
      (d> :new-cluster-size-event-not-alive-node-error (get-id this) e)

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :new-cluster-size-event-unknown-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :new-cluster-size-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :new-cluster-size-event-bad-tx-error (get-id this) e)

      (not (alive-neighbour? sender))
      (d> :new-cluster-size-event-not-alive-neighbour-error (get-id this) e)

      (> alive-number new-size)
      (d> :new-cluster-size-event-less-than-alive-nodes-error (get-id this) e)

      :else
      (do
        (d> :new-cluster-size-event (get-id this) e)
        (set-cluster-size this (.-new_cluster_size e))
        (put-event this e)
        (upsert-neighbour this (assoc sender :tx (.-tx e) :restart-counter (.-restart_counter e)))))))



(defmethod event-processing DeadEvent
  [^NodeObject this ^DeadEvent e]
  (let [sender-id (:id e)
        sender    (or (get-neighbour this sender-id) :unknown-neighbour)]
    (cond

      (= (get-id this) (.-neighbour_id e))
      (when (and
              (alive-neighbour? sender)
              (suitable-restart-counter? this e)
              (suitable-tx? this e))
        (d> :dead-event-about-this-node-error (get-id this) e)
        (set-left-status this))

      (not (alive-node? this))
      (d> :dead-event-not-alive-node-error (get-id this) e)

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :dead-event-unknown-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :dead-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :dead-event-bad-tx-error (get-id this) e)

      (not (alive-neighbour? sender))
      (d> :dead-event-not-alive-neighbour-error (get-id this) e)

      :else
      (let [dead-id (.-neighbour_id e)
            dead-nb (get-neighbour this dead-id)]

        (d> :dead-event (get-id this) e)
        (upsert-neighbour this (assoc sender :tx (.-tx e) :restart-counter (.-restart_counter e)))

        (when (neighbour-exist? this dead-id)
          (when (suitable-restart-counter? this (assoc dead-nb :restart-counter (.-neighbour_restart_counter e)))
            (set-nb-dead-status this dead-id)
            (put-event this e)))))))



;; this event will be not propagated in the cluster so far
(defmethod event-processing SuspectEvent
  [^NodeObject this ^SuspectEvent e]
  (d> :suspect-event (get-id this) e))



(defmethod event-processing LeftEvent
  [^NodeObject this ^LeftEvent e]
  (let [sender-id (:id e)
        sender    (or (get-neighbour this sender-id) :unknown-neighbour)]
    (cond

      (not (alive-node? this))
      (d> :left-event-not-alive-node-error (get-id this) e)

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :left-event-unknown-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :left-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :left-event-bad-tx-error (get-id this) e)

      (not (alive-neighbour? sender))
      (d> :left-event-not-alive-neighbour-error (get-id this) e)

      :else
      (do
        (d> :left-event (get-id this) e)
        (set-nb-left-status this (.-id e))
        (put-event this e)))))



(defmethod event-processing PayloadEvent
  [^NodeObject this ^PayloadEvent e]
  (let [sender-id (:id e)
        sender    (or (get-neighbour this sender-id) :unknown-neighbour)]
    (cond

      (not (alive-node? this))
      (d> :payload-event-not-alive-node-error (get-id this) e)

      (= sender-id (get-id this))
      :ignore-own-event

      (= :unknown-neighbour sender)
      (d> :payload-event-unknown-neighbour-error (get-id this) e)

      (not (suitable-restart-counter? this e))
      (d> :payload-event-bad-restart-counter-error (get-id this) e)

      (not (suitable-tx? this e))
      (d> :payload-event-bad-tx-error (get-id this) e)

      (not (alive-neighbour? sender))
      (d> :payload-event-not-alive-neighbour-error (get-id this) e)

      :else
      (do
        (d> :payload-event (get-id this) e)
        (upsert-neighbour this (assoc sender :tx (.-tx e) :restart-counter (.-restart_counter e) :payload (.-payload e)))
        (put-event this e)))))




(defmethod event-processing :default
  [^NodeObject this e]
  (d> :event-processing-default (get-id this) {:msg "Unknown event type" :event e}))


;;;;


(defn udp-packet-processor
  "Main function to process all incoming UDP packets. UDP packets will be decrypted, deserialized to events.
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
          (d> :udp-packet-processor (get-id this) {:event event})
          (event-processing this event)))
      (d> :udp-packet-processor-error (get-id this) {:msg             "Bad events vector structure"
                                                     :events-vector   events-vector
                                                     :bad-udp-counter (:bad-udp-counter
                                                                        (swap! *stat update :bad-udp-counter inc))}))))


;; TODO: run periodic process for clean probe ack events - remember uuid key for non empty maps.
;; On next iteration remembered uuids should be cleaned.

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



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; NodeObject control functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn node-start
  "Start the node and run `node-process-fn` in a separate virtual thread.
   Params:
    * `node-process-fn` - fn with one arg `this` for main node process. It may look for :continue? flag in UDP server.
    * `incoming-data-processor-fn` fn to process incoming UDP packets with two args: `this` and `encrypted-data`"
  [^NodeObject this node-process-fn incoming-data-processor-fn]
  (try
    (when (= (get-status this) :stop)
      (set-left-status this)
      (swap! (:*node this) assoc :tx 1)
      (let [{:keys [host port]} (get-value this)]
        (swap! (:*node this) assoc :*udp-server (udp/start host port (partial incoming-data-processor-fn this))))
      (when-not (s/valid? ::spec/node (get-value this))
        (throw (ex-info "Invalid node data" (->> this :*node (s/explain-data ::spec/node) spec/problems))))
      (swap! *stat assoc :bad-udp-counter 0)
      (vthread (node-process-fn this))
      (d> :start (get-id this) {}))
    (catch Exception e
      (throw (ex-info (format "Can't start node: %s" (get-id this)) {:cause (ex-cause e)} e)))))



(defn node-probe
  "Send probe event to neighbour. Put probe key to probe events map.
  Returns probe key."
  ^UUID [^NodeObject this ^String neighbour-host ^long neighbour-port]
  (let [probe-event (new-probe-event this neighbour-host neighbour-port)]
    (d> :probe (get-id this) probe-event)
    (insert-probe this probe-event)
    (send-event this probe-event neighbour-host neighbour-port)
    (.-probe_key probe-event)))


(defn stop-rejoin-watcher
  "Stop rejoin watcher."
  [^NodeObject this & {:keys [auto-rejoin?] :or {auto-rejoin? false}}]
  (remove-watch (:*node this) :rejoin-watcher)
  (if auto-rejoin?
    (d> :remove-previous-rejoin-watcher (get-id this) {})
    (d> :rejoin-watcher-stop (get-id this) {})))


;; TODO: stop process of periodic event send from buffer
(defn node-leave
  "Leave the cluster"
  [^NodeObject this]
  (if (= :left (get-status this))
    true
    (let [_ (stop-rejoin-watcher this)
          n             (calc-n (nodes-in-cluster this))
          neighbour-ids (take-ids-for-ping this n)
          left-event    (new-left-event this)]
      (doseq [nb-id neighbour-ids]
        (send-event this left-event nb-id))
      (set-left-status this)
      true)))




;; FIXME: start process of periodic event send from buffer


(defn indirect-ack-timeout-watcher
  "Detect indirect ack timeout. Should run in a separate virtual thread.
  Returns void."
  [^NodeObject this neighbour-id ts & {:keys [ack-timeout-ms max-ping-without-ack-before-dead] :or
                                       {ack-timeout-ms (-> @*config :ack-timeout-ms)
                                        max-ping-without-ack-before-dead (-> @*config :max-ping-without-ack-before-dead)}}]

  (Thread/sleep ^Long ack-timeout-ms)
  (when-let [indirect-ping-event (get-indirect-ping-event this neighbour-id)]
    (let [attempt-number (.-attempt_number indirect-ping-event)]
      (when (= ts (.-ts indirect-ping-event))
        (d> :indirect-ack-timeout (get-id this) {:neighbour-id neighbour-id :attempt-number attempt-number})
        (delete-indirect-ping this neighbour-id)
        (if (< attempt-number max-ping-without-ack-before-dead)
          (let [alive-neighbours   (get-alive-neighbours this)
                alive-nodes-number (count alive-neighbours)]
            (if (pos-int? alive-nodes-number)
              (let [random-alive-nb          (rand-nth alive-neighbours)
                    next-indirect-ping-event (new-indirect-ping-event this (:id random-alive-nb) neighbour-id (inc attempt-number))]
                (upsert-indirect-ping this next-indirect-ping-event)
                (vthread (indirect-ack-timeout-watcher this neighbour-id (.-ts next-indirect-ping-event)))
                (send-event this next-indirect-ping-event neighbour-id))
              (do
                (set-nb-dead-status this neighbour-id)
                (put-event this (new-dead-event this neighbour-id)))))
          (do
            (set-nb-dead-status this neighbour-id)
            (put-event this (new-dead-event this neighbour-id))))))))



(defn ack-timeout-watcher
  "Detect ack timeout. Should run in a separate virtual thread.
  Returns void."
  [^NodeObject this neighbour-id ts & {:keys [ack-timeout-ms max-ping-without-ack-before-suspect] :or
                                       {ack-timeout-ms (-> @*config :ack-timeout-ms)
                                        max-ping-without-ack-before-suspect (-> @*config :max-ping-without-ack-before-suspect)}}]

  (Thread/sleep ^Long ack-timeout-ms)
  (when-let [ping-event (get-ping-event this [neighbour-id ts])]
    (let [attempt-number (.-attempt_number ping-event)]
      (d> :ack-timeout (get-id this) {:neighbour-id neighbour-id :attempt-number attempt-number})
      (delete-ping this [neighbour-id ts])
      (if (< attempt-number max-ping-without-ack-before-suspect)
        (let [next-ping-event (new-ping-event this neighbour-id (inc attempt-number))]
          (insert-ping this next-ping-event)
          (vthread (ack-timeout-watcher this neighbour-id (.-ts next-ping-event)))
          (send-event this next-ping-event neighbour-id))
        (let [_                  (set-nb-suspect-status this neighbour-id)
              alive-neighbours   (get-alive-neighbours this)
              alive-nodes-number (count alive-neighbours)]
          (if (zero? alive-nodes-number)
            (set-nb-dead-status this neighbour-id)
            (let [random-alive-nb     (rand-nth alive-neighbours)
                  indirect-ping-event (new-indirect-ping-event this (:id random-alive-nb) neighbour-id (inc attempt-number))]
              (upsert-indirect-ping this indirect-ping-event)
              (vthread (indirect-ack-timeout-watcher this neighbour-id (.-ts indirect-ping-event)))
              (send-event this indirect-ping-event (:id random-alive-nb)))))))))



(defn ping-heartbeat
  "Should run in a separate virtual thread."
  [^NodeObject this & {:keys [ping-heartbeat-ms] :or {ping-heartbeat-ms (-> @*config :ping-heartbeat-ms)}}]
  (while (alive-node? this)
    (try
      (let [events        (take-events this)
            n             (calc-n (nodes-in-cluster this))
            neighbour-ids (take-ids-for-ping this n)]
        (doseq [neighbour-id neighbour-ids]
          (let [nb         (get-neighbour this neighbour-id)
                ping-event (new-ping-event this neighbour-id 1)
                nb-events  (conj events ping-event)]
            (insert-ping this ping-event)
            (vthread (ack-timeout-watcher this neighbour-id (.-ts ping-event)))
            (send-events this nb-events (.-host nb) (.-port nb))
            (d> :ping-heartbeat (get-id this) {:known-nodes-number  (count (get-alive-neighbours this))
                                               :events-sent-number  (count nb-events)
                                               :active-pings-number (count (get-ping-events this))
                                               :ping-heartbeat-ms   ping-heartbeat-ms}))))
      (catch Exception e
        (d> :ping-heartbeat-error (get-id this) {:message (ex-message e)})))
    (Thread/sleep ^Long ping-heartbeat-ms)))


(declare node-join)


(defn start-rejoin-watcher
  "Start rejoin watcher. This watcher will start only if node has alive status.
  This watcher intended to rejoin node to cluster if it considered as dead.
  Watcher detect :left node status when dead event arrives and rejoin to cluster."
  [^NodeObject this & {:keys [rejoin-if-dead? rejoin-max-attempts]
                       :or {rejoin-if-dead?     (-> @*config :rejoin-if-dead?)
                            rejoin-max-attempts (-> @*config :rejoin-max-attempts)}}]
  (when (alive-node? this)
    (add-watch (:*node this) :rejoin-watcher
      (fn [_ _ old-state new-state]
        (when (and
                (= :alive (:status old-state))
                (= :left (:status new-state))
                rejoin-if-dead?)

          (loop [attempt 0]
            (d> :rejoin-attempt (get-id this) {:attempts attempt})
            (stop-rejoin-watcher this {:auto-rejoin? true})
            (if (node-join this)
              (d> :rejoin-complete (get-id this) {:attempts attempt})
              (if (>= attempt rejoin-max-attempts)
                (d> :rejoin-max-attempts-reached-error (get-id this) {:attempts attempt})
                (recur (inc attempt))))))))))


(defn node-join
  "Join this node to the cluster. Blocks thread until join confirmation from alive nodes.
   If status is already :alive or :join then returns nil and do nothing.

   Increase restart counter. Set tx to 0.

   If cluster size > 1:
    1. Set status for this node as :join.
    2. Notify known alive neighbours for this node
    3. Block thread and wait for join confirmation event from alive nodes.
       When status became alive or timeout happens (max-join-time-ms) continue thread execution.
    4. If join fails then set status :left
    5. If join success then start rejoin watcher if enabled.

   If cluster size = 1:
    0. Delete all neighbours info
    1. Set status for this node as :alive and become single node in the cluster
    2. Start auto rejoin watcher if enabled

  Returns true if join complete, false if join fails due to timeout or nil if already has join or alive status"
  [^NodeObject this & {:keys [max-join-time-ms rejoin-if-dead?]
                       :or {max-join-time-ms (-> @*config :max-join-time-ms)
                            rejoin-if-dead?  (-> @*config :rejoin-if-dead?)}}]
  (when-not (or (alive-node? this) (= :join (get-status this)))

    (set-restart-counter this (inc (get-restart-counter this)))
    (set-tx this 0)

    (let [cluster-size (get-cluster-size this)]
      (cond

        (= 1 cluster-size)
        (do
          (set-join-status this)
          (d> :join (get-id this) {:cluster-size cluster-size})
          (delete-neighbours this)
          (set-alive-status this)
          (when rejoin-if-dead? (start-rejoin-watcher this))
          (vthread (ping-heartbeat this))
          true)

        (> cluster-size 1)
        (let [n             (calc-n cluster-size)
              join-event    (new-join-event this)
              alive-nb-ids  (mapv :id (get-alive-neighbours this))
              nb-random-ids (take n (shuffle alive-nb-ids))
              *join-await-promise (promise)]

          (set-join-status this)

          (add-watch (:*node this) :join-await-watcher
            (fn [_ _ _ new-state]
              (when (= :alive (:status new-state))
                (deliver *join-await-promise :alive))))

          (when (seq nb-random-ids)
            (run!
              (fn [nb-id] (send-event this join-event nb-id))
              nb-random-ids))

          (d> :join (get-id this) {:cluster-size        cluster-size
                                   :notified-neighbours (vec nb-random-ids)})

          (if (alive-node? this)
            (do
              (remove-watch (:*node this) :join-await-watcher)
              (when rejoin-if-dead? (start-rejoin-watcher this))
              (vthread (ping-heartbeat this))
              true)
            (do
              (deref *join-await-promise max-join-time-ms :timeout)
              (remove-watch (:*node this) :join-await-watcher)
              (if (alive-node? this)
                (do
                  (when rejoin-if-dead? (start-rejoin-watcher this))
                  (vthread (ping-heartbeat this))
                  true)
                (do
                  (set-left-status this)
                  false)))))))))



(defn node-ping
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
      (insert-ping this ping-event)
      (send-events this events-vector nb-host nb-port)
      ping-event)
    (do
      (d> :ping-unknown-neighbour-id-error (get-id this) {:neighbour-id neighbour-id})
      :ping-unknown-neighbour-id-error)))


(defn node-stop
  "Stop the node and leave the cluster"
  [^NodeObject this]
  (if (= :stop (get-status this))
    true
    (let [{:keys [*udp-server]} (get-value this)]
      (node-leave this)
      (swap! (:*node this) assoc
        :*udp-server (when *udp-server (udp/stop *udp-server))
        :ping-events {}
        :outgoing-event-queue []
        :ping-round-buffer []
        :tx 0)
      (set-stop-status this)
      (when-not (s/valid? ::spec/node (get-value this))
        (throw (ex-info "Invalid node data" (spec/problems (s/explain-data ::spec/node (:*node this))))))))
  (d> :stop (get-id this) {}))



(defn node-payload
  "Get node payload"
  [^NodeObject this]
  (get-payload this))


(defn node-payload-update
  "Set node payload and send event with new payload to other nodes"
  [^NodeObject this new-payload]
  (set-payload this new-payload)
  (put-event this (new-payload-event this)))
