(ns org.rssys.swim
  "SWIM functions, specs and domain entities"
  (:require
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
      AntiEntropy
      DeadEvent
      PingEvent
      ProbeAckEvent
      ProbeEvent)))


(def ^:dynamic *enable-diag-tap?*
  "Put diagnostic data to tap>"
  true)


(defn d>
  "If `*enable-diag-tap?*` is true (default), then put diagnostic data to tap>.
   Returns true if there was room in the tap> queue, false if not (dropped),
   nil if `*enable-diag-tap?*` disabled."
  [cmd-kw node-id data]
  (when *enable-diag-tap?*
    (tap> {::cmd    cmd-kw
           :node-id node-id
           :data    data})))


(defmacro safe
  [& body]
  `(try
     ~@body
     (catch Exception e#)))


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


;;;;;;;;;;;;;;;;;;;
;; Domain entities
;;;;;;;;;;;;;;;;;;;

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


;; TODO:
;; How to clean neighbour table from old nodes?
;;


(defn value
  "Get node value"
  ^Node
  [^NodeObject this]
  @(:*node this))


(defn get-id
  "Get node id"
  ^UUID
  [^NodeObject this]
  (.-id (value this)))


(defn host
  "Get node host"
  ^String
  [^NodeObject this]
  (.-host (value this)))


(defn port
  "Get node port"
  ^long
  [^NodeObject this]
  (.-port (value this)))


(defn restart-counter
  "Get node restart counter"
  ^long
  [^NodeObject this]
  (.-restart_counter (value this)))


(defn tx
  "Get node tx"
  ^long [^NodeObject this]
  (.-tx (value this)))


(defn cluster
  "Get cluster value"
  ^Cluster
  [^NodeObject this]
  (.-cluster (value this)))


(defn cluster-size
  "Get cluster size"
  ^long [^NodeObject this]
  (.-cluster_size (cluster this)))


(defn payload
  "Get node payload"
  [^NodeObject this]
  (:payload (value this)))


(defn neighbour
  "Get neighbour by id"
  ^NeighbourNode
  [^NodeObject this ^UUID id]
  (get (.-neighbours (value this)) id))


(defn neighbours
  "Get all node neighbours"
  [^NodeObject this]
  (.-neighbours (value this)))


(defn status
  "Get current node status"
  ^Keyword
  [^NodeObject this]
  (.-status (value this)))


(defn outgoing-event-queue
  "Get vector of prepared outgoing events"
  [^NodeObject this]
  (.-outgoing_event_queue (value this)))


(defn ping-events
  "Get map of active ping events which we sent to neighbours"
  [^NodeObject this]
  (.-ping_events (value this)))


(defn ping-event
  "Get active ping event by neighbour id if exist"
  ^PingEvent
  [^NodeObject this neighbour-id]
  (get (ping-events this) neighbour-id))


(defn set-cluster
  "Set new cluster for this node"
  [^NodeObject this ^Cluster cluster]
  (cond
    (not (s/valid? ::spec/cluster cluster))
    (throw (ex-info "Invalid cluster data" (->> cluster (s/explain-data ::spec/cluster) spec/problems)))

    (not= :stop (status this))
    (throw (ex-info "Node is not stopped. Can't set new cluster value." {:current-status (status this)}))

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
  (swap! (:*node this) assoc :cluster (assoc (cluster this) :cluster-size new-cluster-size)))


(defn set-status
  "Set new status for this node"
  [^NodeObject this ^Keyword new-status]
  (when-not (s/valid? ::spec/status new-status)
    (throw (ex-info "Invalid node status" (->> new-status (s/explain-data ::spec/status) spec/problems))))
  (d> :set-status (get-id this) {:new-status new-status})
  (swap! (:*node this) assoc :status new-status))


(defn set-payload
  "Set new payload for this node"
  [^NodeObject this payload]
  ;;TODO: send event to cluster about new payload
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
  (swap! (:*node this) assoc :tx (inc (tx this))))


(defn upsert-neighbour
  "Update existing or insert new neighbour to neighbours map"
  [^NodeObject this ^NeighbourNode neighbour-node]
  (when-not (s/valid? ::spec/neighbour-node neighbour-node)
    (throw (ex-info "Invalid neighbour node data"
             (->> neighbour-node (s/explain-data ::spec/neighbour-node) spec/problems))))
  (d> :upsert-neighbour (get-id this) {:neighbour-node neighbour-node})
  (swap! (:*node this) assoc :neighbours (assoc
                                           (neighbours this)
                                           (.-id neighbour-node)
                                           (assoc neighbour-node :updated-at (System/currentTimeMillis)))))


(defn delete-neighbour
  "Delete neighbour from neighbours map"
  [^NodeObject this ^UUID neighbour-id]
  (d> :delete-neighbour (get-id this) {:neighbour-id neighbour-id})
  (swap! (:*node this) assoc :neighbours (dissoc (neighbours this) neighbour-id)))


(defn set-outgoing-event-queue
  "Set outgoing events queue new value"
  [^NodeObject this new-event-queue]
  (when-not (s/valid? ::spec/outgoing-event-queue new-event-queue)
    (throw (ex-info "Invalid outgoing event queue data"
             (->> new-event-queue (s/explain-data ::spec/outgoing-event-queue) spec/problems))))
  (d> :set-outgoing-event-queue (get-id this) {:new-event-queue new-event-queue})
  (swap! (:*node this) assoc :outgoing-event-queue new-event-queue))


(defn put-event
  "Put prepared outgoing event to queue (FIFO)"
  [^NodeObject this prepared-event]
  (when-not (vector? prepared-event)
    (throw (ex-info "Event should be a vector (prepared event)" {:prepared-event prepared-event})))
  (d> :put-event (get-id this) {:prepared-event prepared-event :tx (tx this)})
  (swap! (:*node this) assoc :outgoing-event-queue
    (conj (outgoing-event-queue this) prepared-event) :tx (tx this)))


(defn take-event
  "Take one prepared outgoing event from queue (FIFO).
  Taken event will be removed from queue."
  [^NodeObject this]
  (let [event (first (outgoing-event-queue this))]
    (swap! (:*node this) assoc :outgoing-event-queue (->> this outgoing-event-queue rest vec))
    event))


;; NB ;; the group-by [:id :restart-counter :tx] and send the latest events only
(defn take-events
  "Take `n` prepared outgoing events from queue (FIFO).
  Taken events will be removed from queue."
  [^NodeObject this ^long n]
  (let [events (->> this outgoing-event-queue (take n) vec)]
    (swap! (:*node this) assoc :outgoing-event-queue (->> this outgoing-event-queue (drop n) vec))
    events))


(defn upsert-ping
  "Update existing or insert new active ping event in map"
  [^NodeObject this ^PingEvent ping-event]
  (when-not (s/valid? ::spec/ping-event ping-event)
    (throw (ex-info "Invalid ping event data" (->> ping-event (s/explain-data ::spec/ping-event) spec/problems))))
  (d> :upsert-ping (get-id this) {:ping-event ping-event})
  (swap! (:*node this) assoc :ping-events (assoc (ping-events this) (:neighbour-id ping-event) ping-event)))


(defn delete-ping
  "Delete active ping event from map"
  [^NodeObject this ^UUID neighbour-id]
  (d> :delete-ping (get-id this) {:neighbour-id neighbour-id})
  (swap! (:*node this) assoc :ping-events (dissoc (ping-events this) neighbour-id)))


;; NB: `node-process-fn` is a fn with one arg - [this], `udp-dispatcher-fn` is fn with two args: [this, udp-received-data]
;; `node-process-fn` looks for :continue? flag in UDP server. If it's false then `node-process-fn` terminates.
(defn start
  "Start node and use `node-process-fn` as main node process and `udp-dispatcher-fn` to process incoming UDP packets."
  [^NodeObject this node-process-fn udp-dispatcher-fn]
  (when (= (status this) :stop)
    (set-status this :left)
    (set-restart-counter this (inc (restart-counter this)))
    (let [{:keys [host port]} (value this)]
      (swap! (:*node this) assoc :*udp-server (udp/start host port (partial udp-dispatcher-fn this))))
    (when-not (s/valid? ::spec/node (value this))
      (throw (ex-info "Invalid node data" (->> this :*node (s/explain-data ::spec/node) spec/problems))))
    (vthread/vfuture (node-process-fn this))
    (d> :start (get-id this) {})))


(defn leave
  "Leave the cluster"
  [^NodeObject this]
  ;;TODO
  )


(defn join
  "Join this node to the cluster"
  [^NodeObject this]
  ;;TODO
  )


(defn probe
  "Probe other node and if its alive then put it to a neighbours table"
  [^NodeObject this ^String host ^long port]
  ;;TODO
  )


;; NB: if in Ack id is different, then send event and change id in a neighbours table
(defn ping
  "Send Ping event to neighbour node"
  [^NodeObject this neighbour-id]
  ;;TODO
  )


(defn ack
  "Send Ack event to neighbour node"
  [^NodeObject this ^PingEvent ping-event]
  ;;TODO
  )


(defn probe-ack
  "Send Ack event to neighbour node"
  [^NodeObject this ^ProbeAckEvent probe-ack-event]
  ;;TODO
  )


(defn stop
  "Stop the node and leave the cluster"
  [^NodeObject this]
  (let [{:keys [*udp-server scheduler-pool]} (value this)]
    (leave this)
    (scheduler/stop-and-reset-pool! scheduler-pool :strategy :kill)
    (swap! (:*node this) assoc
      :*udp-server (udp/stop *udp-server)
      :ping-events {}
      :outgoing-event-queue []
      :ping-round-buffer []
      :tx 0)
    (set-status this :stop)
    (when-not (s/valid? ::spec/node (value this))
      (throw (ex-info "Invalid node data" (spec/problems (s/explain-data ::spec/node (:*node this)))))))
  (d> :stop (get-id this) {}))


;;;;;;;;;;
;; Event builders
;;;;;;;;;;

(defn ping-event
  "Returns new ping event"
  ^PingEvent [^NodeObject this ^UUID neighbour-id attempt-number]
  (let [ping-event (event/map->PingEvent {:cmd-type        (:ping event/code)
                                          :id              (get-id this)
                                          :host            (host this)
                                          :port            (port this)
                                          :restart-counter (restart-counter this)
                                          :tx              (tx this)
                                          :neighbour-id    neighbour-id
                                          :attempt-number  attempt-number})]
    (if-not (s/valid? ::spec/ping-event ping-event)
      (throw (ex-info "Invalid ping event" (spec/problems (s/explain-data ::spec/ping-event ping-event))))
      ping-event)))


;;;;

(defn ack-event
  "Returns new Ack event"
  ^AckEvent [^NodeObject this ^PingEvent e]
  (let [ack-event (event/map->AckEvent {:cmd-type        (:ack event/code)
                                        :id              (get-id this)
                                        :restart-counter (restart-counter this)
                                        :tx              (tx this)
                                        :neighbour-id    (.-id e)
                                        :neighbour-tx    (.-tx e)})]
    (if-not (s/valid? ::spec/ack-event ack-event)
      (throw (ex-info "Invalid ack event" (spec/problems (s/explain-data ::spec/ack-event ack-event))))
      ack-event)))


;;;;;

(defn dead-event
  "Returns new dead event"
  ^DeadEvent [^NodeObject this ^PingEvent e]
  (let [dead-event (event/map->DeadEvent {:cmd-type        (:dead event/code)
                                          :id              (get-id this)
                                          :restart-counter (restart-counter this)
                                          :tx              (tx this)
                                          :neighbour-id    (.-id e)
                                          :neighbour-tx    (.-tx e)})]
    (if-not (s/valid? ::spec/dead-event dead-event)
      (throw (ex-info "Invalid dead event" (spec/problems (s/explain-data ::spec/dead-event dead-event))))
      dead-event)))


;;;;

(defn probe-event
  "Returns new probe event"
  ^ProbeEvent [^NodeObject this ^String neighbour-host ^long neighbour-port]
  (let [probe-event (event/map->ProbeEvent {:cmd-type        (:probe event/code)
                                            :id              (get-id this)
                                            :host            (host this)
                                            :port            (port this)
                                            :restart-counter (restart-counter this)
                                            :tx              (tx this)
                                            :neighbour-host  neighbour-host
                                            :neighbour-port  neighbour-port})]
    (if-not (s/valid? ::spec/probe-event probe-event)
      (throw (ex-info "Invalid probe event" (spec/problems (s/explain-data ::spec/probe-event probe-event))))
      probe-event)))


;;;;



(defn probe-ack-event
  "Returns new probe ack event"
  ^ProbeAckEvent [^NodeObject this ^ProbeEvent e]
  (let [ack-event (event/map->ProbeAckEvent {:cmd-type        (:probe-ack event/code)
                                             :id              (get-id this)
                                             :restart-counter (restart-counter this)
                                             :tx              (tx this)
                                             :neighbour-id    (.-id e)
                                             :neighbour-tx    (.-tx e)})]
    (if-not (s/valid? ::spec/probe-ack-event ack-event)
      (throw (ex-info "Invalid probe ack event" (spec/problems (s/explain-data ::spec/probe-ack-event ack-event))))
      ack-event)))


;;;;

(defn build-anti-entropy-data
  "Build anti-entropy data – subset of known nodes from neighbours map.
  This data is propagated from node to node and thus nodes can get knowledge about unknown nodes.
  To apply anti-entropy data receiver should compare incarnation pair [restart-counter tx] and apply only
  if node has older data.
  Returns vector of known neighbors size of `num` if any or empty vector."
  [^NodeObject this & {:keys [num] :or {num 2}}]
  (or
    (some->>
      (neighbours this)
      vals
      shuffle
      (take num)
      (map #(into {} %))
      vec)
    []))


(defn anti-entropy-event
  "Returns anti-entropy event"
  ^AntiEntropy [^NodeObject this]
  (let [anti-entropy-data (build-anti-entropy-data this)
        ae-event          (event/map->AntiEntropy {:cmd-type          (:anti-entropy event/code)
                                                   :anti-entropy-data anti-entropy-data})]
    (if-not (s/valid? ::spec/anti-entropy-event ae-event)
      (throw (ex-info "Invalid anti-entropy event" (spec/problems (s/explain-data ::spec/anti-entropy-event ae-event))))
      ae-event)))


(defn empty-anti-entropy
  "Returns empty anti-entropy event"
  ^AntiEntropy []
  (event/map->AntiEntropy {:cmd-type          (:anti-entropy event/code)
                           :anti-entropy-data []}))


;;;;;


(defn node-process-fn
  [^NodeObject this]
  (let [*idx (atom 0)
        rot  ["\\" "|" "/" "—"]]
    (Thread/sleep 100)
    (while (-> this value :*udp-server deref :continue?)
      (print (format "\rNode is active: %s  " (nth rot (rem @*idx (count rot)))))
      (Thread/sleep 200)
      (swap! *idx inc)
      (flush)))
  (println "Node is stopped."))


;;;;

(defmulti restore-event (fn [x] (.get ^PersistentVector x 0)))

(defmethod restore-event 0 ^PingEvent [e] (.restore (event/empty-ping) e))
(defmethod restore-event 1 ^AckEvent [e] (.restore (event/empty-dead) e))
(defmethod restore-event 8 ^AntiEntropy [e] (.restore (event/empty-anti-entropy) e))
(defmethod restore-event 9 ^ProbeEvent [e] (.restore (event/empty-probe) e))


;;;;


(defn suitable-restart-counter?
  "Check that restart counter from neighbours map is less or equal than from event.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this e]
  (let [neighbour-id (:id e)]
    (when-let [nb ^NeighbourNode (neighbour this neighbour-id)]
      (<= (.-restart_counter nb) (:restart-counter e)))))


(defn suitable-tx?
  "Check that tx from neighbours map is less or equal than from event.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this e]
  (let [neighbour-id (:id e)]
    (when-let [nb ^NeighbourNode (neighbour this neighbour-id)]
      (<= (.-tx nb) (:tx e)))))


(defn suitable-incarnation?
  "Check that incarnation, pair [restart-counter tx] from neighbours map is less or equal than from event.
  Returns true, if suitable and false if not."
  [^NodeObject this e]
  (= [true true] [(suitable-restart-counter? this e) (suitable-tx? this e)]))


;;;;;;;;;;

(defn send-event-only
  "Send one event to neighbour"
  [^NodeObject this e neighbour-host neighbour-port]
  (let [data ^bytes (e/encrypt-data (-> this cluster :secret-key) (serialize [e]))]
    (udp/send-packet data neighbour-host neighbour-port)))


(defn send-event-with-anti-entropy
  "Send one event + anti entropy data to neighbour"
  [^NodeObject this e neighbour-host neighbour-port]
  (let [data ^bytes (e/encrypt-data (-> this cluster :secret-key) (serialize [e (anti-entropy-event this)]))]
    (udp/send-packet data neighbour-host neighbour-port)))



(defmulti process-incoming-event (fn [this e] (type e)))


(defmethod process-incoming-event ProbeEvent
  [^NodeObject this ^ProbeEvent e]

  (when (not (neighbour this (.-id e)))
    (let [new-neighbour (new-neighbour-node {:id              (.-id e)
                                             :host            (.-host e)
                                             :port            (.-port e)
                                             :status          :left
                                             :access          :direct
                                             :restart-counter (.-restart_counter e)
                                             :tx              (.-tx e)
                                             :payload         {}
                                             :updated-at      (System/currentTimeMillis)})]

      (d> :process-incoming-event-probe-add-new-neighbour (get-id this) new-neighbour)
      (upsert-neighbour this new-neighbour)))

  (inc-tx this)                                             ;; every event on node increments tx
  (send-event-with-anti-entropy this (probe-ack-event this e) (.-host e) (.-port e)))


(defmethod process-incoming-event PingEvent
  [^NodeObject this ^PingEvent e]

  ;; add new neighbour if it not exists in neighbours map
  ;; TODO  переделать на проверку статуса dead т.к. это нарушает стейт-машину (нарисовать!)
  (when (not (neighbour this (.-id e)))
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
    (let [dead-event (dead-event this e)]
      (inc-tx this)                                         ;; every event on node increments tx
      (d> :process-incoming-event-ping-dead-event (get-id this) dead-event)
      (send-event-only this dead-event (.-host e) (.-port e)))

    (not (suitable-tx? this e)) :do-nothing

    :else
    (let [ack-event         (ack-event this e)
          anti-entropy-data :todo]
      (inc-tx this)                                         ;; every event on node increments tx
      (d> :process-incoming-event-ping-ack-event (get-id this) ack-event)
      (send-event-with-anti-entropy this ack-event (.-host e) (.-port e))))


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
  (neighbours this)
  (println (tx this)))


(defmethod process-incoming-event :default
  [^NodeObject this e]
  (d> :process-incoming-event-default (get-id this) {:msg "Unknown event type" :event e}))


;;;;

(defn udp-dispatcher-fn
  [^NodeObject this ^bytes udp-data]
  (let [secret-key     (-> this cluster :secret-key)
        decrypted-data (safe (e/decrypt-data ^bytes secret-key ^bytes udp-data)) ;; Ignore bad messages
        events-vector  (deserialize ^bytes decrypted-data)]
    (if (vector? events-vector)
      (doseq [serialized-event events-vector]
        (let [event (restore-event serialized-event)]
          (inc-tx this)                                     ;; Every incoming event must increment tx
          (d> :process-incoming-event (get-id this) {:event event})
          (process-incoming-event this event)))
      (d> :udp-dispatcher-fn (get-id this) {:msg "Bad events vector structure" :events-vector events-vector}))))




