(ns org.rssys.swim
  "SWIM functions, specs and domain entities"
  (:require
    [clojure.spec.alpha :as s]
    [cognitect.transit :as transit]
    [org.rssys.common :as common]
    [org.rssys.encrypt :as e]
    [org.rssys.scheduler :as scheduler]
    [org.rssys.udp :as udp])
  (:import
    (clojure.lang
      PersistentVector)
    (java.io
      ByteArrayInputStream
      ByteArrayOutputStream
      Writer)
    (java.util
      UUID)))


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


;;;;;;;;;;;;;;;;
;; SWIM spec
;;;;;;;;;;;;;;;;

(s/def ::id uuid?)
(s/def ::neighbour-id ::id)
(s/def ::host string?)
(s/def ::port (s/and pos-int? #(< % 65536)))
(s/def ::neighbour-host ::host)
(s/def ::neighbour-port ::port)
(s/def ::name string?)
(s/def ::desc string?)
(s/def ::status #{:stop :join :alive :suspect :left :dead :unknown})
(s/def ::access #{:direct :indirect})
(s/def ::object any?)
(s/def ::tags set?)                                         ;; #{"dc1" "test"}
(s/def ::nspace (s/or :symbol symbol? :keyword keyword? :string string?)) ;; cluster namespace
(s/def ::secret-token string?)                              ;; string token for secret key gen to access to cluster
(s/def ::secret-key ::object)                               ;; 256-bit SecretKey generated from secret token
(s/def ::cluster-size nat-int?)                             ;; number of nodes in the cluster
(s/def ::cluster (s/keys :req-un [::id ::name ::desc ::secret-token ::nspace ::tags] :opt-un [::secret-key ::cluster-size]))

(s/def ::restart-counter nat-int?)                          ;; increase every node restart. part of incarnation.
(s/def ::tx nat-int?)                                       ;; increase every event on node. part of incarnation.
(s/def ::payload ::object)                                  ;; some data attached to node and propagated to cluster
(s/def ::updated-at nat-int?)
(s/def ::neighbour-node (s/keys :req-un [::id ::host ::port ::status ::access ::restart-counter ::tx ::payload ::updated-at]))
(s/def ::neighbours (s/map-of ::neighbour-id ::neighbour-node))

(s/def ::anti-entropy-data (s/coll-of ::neighbour-node))

(s/def ::attempt-number pos-int?)
(s/def ::ping-event (s/keys :req-un [::cmd-type ::id ::host ::port ::restart-counter ::tx ::neighbour-id ::attempt-number]))
(s/def ::ping-events (s/map-of ::neighbour-id ::ping-event)) ;; buffer is used to remember whom we sent a ping and waiting for an ack

(s/def ::neighbour-tx ::tx)
(s/def ::ack-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::neighbour-id ::neighbour-tx]))
(s/def ::probe-ack-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::neighbour-id ::neighbour-tx]))


;; ::neighbour-id - dead node
(s/def ::dead-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::neighbour-id ::neighbour-tx]))

(s/def ::probe-event (s/keys :req-un [::cmd-type ::id ::host ::port ::restart-counter ::tx ::neighbour-host ::neighbour-port]))
(s/def ::anti-entropy-event (s/keys :req-un [::cmd-type ::anti-entropy-data]))

(s/def ::scheduler-pool ::object)
(s/def ::*udp-server ::object)
(s/def ::event-queue vector?)                               ;; buffer for outgoing events which we propagate with ping and ack events
(s/def ::ping-round-buffer (s/coll-of ::neighbour-id))


(s/def ::node
  (s/keys :req-un [::id ::host ::port ::cluster ::status ::neighbours ::restart-counter
                   ::tx ::ping-events ::payload ::scheduler-pool ::*udp-server ::event-queue
                   ::ping-round-buffer]))


(def event-code
  {:ping 0 :ack 1 :join 2 :alive 3 :suspect 4 :left 5 :dead 6 :payload 7 :anti-entropy 8 :probe 9 :probe-ack 10})


(defn spec-problems
  [explain-data]
  {:problems (vec (::s/problems explain-data))})


;;;;;;;;;;;;

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

(defn cluster-str
  "Returns String representation of Cluster"
  ^String
  [cluster]
  (str (into {} (assoc cluster :secret-token "***censored***"))))


(defrecord Cluster [id name desc secret-token nspace tags secret-key cluster-size]
           Object
           (toString [this] (cluster-str this)))


(defn new-cluster
  "Returns new Cluster instance."
  ^Cluster [{:keys [id name desc secret-token nspace tags cluster-size] :as c}]
  (when-not (s/valid? ::cluster c)
    (throw (ex-info "Invalid cluster data" (->> c (s/explain-data ::cluster) spec-problems))))
  (map->Cluster {:id           (or id (random-uuid)) :name name :desc desc :secret-token secret-token
                 :nspace       nspace :tags tags :secret-key (e/gen-secret-key secret-token)
                 :cluster-size (or cluster-size 1)}))


(defmethod print-method Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


(defmethod print-dup Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


;;;;;;;;;


(defrecord NeighbourNode [id host port status access restart-counter tx payload updated-at])


(defn new-neighbour-node
  "Returns new NeighbourNode instance."

  (^NeighbourNode [neighbour-map]
    (if-not (s/valid? ::neighbour-node neighbour-map)
      (throw (ex-info "Invalid neighbour data" (->> neighbour-map (s/explain-data ::neighbour-node) spec-problems)))
      (map->NeighbourNode neighbour-map)))


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



(defrecord Node [id host port cluster status neighbours restart-counter tx ping-events
                 payload scheduler-pool *udp-server event-queue ping-round-buffer]
           Object
           (toString [this] (.toString this)))


(defprotocol ISwimNode
  "SWIM Node protocol"
  :extend-via-metadata true

  ;; Getters
  (value [this] "Get node value")
  (id [this] "Get node id")
  (host [this] "Get node host")
  (port [this] "Get node port")
  (restart-counter [this] "Get node restart counter")
  (tx [this] "Get node tx")
  (cluster [this] "Get cluster value")
  (cluster-size [this] "Get cluster size")
  (payload [this] "Get node payload")
  (neighbour [this neighbour-id] "Get neighbour by id")
  (neighbours [this] "Get all neighbours")
  (status [this] "Get current node status")
  (event-queue [this] "Get vector of prepared outgoing events")
  (ping-event [this neighbour-id] "Get active ping event by neighbour id if exist")
  (ping-events [this] "Get map of active ping events which we sent to neighbours")

  ;; Setters
  (set-cluster [this cluster] "Set new cluster for this node")
  (set-status [this new-status] "Set new status for this node")
  (set-cluster-size [this new-cluster-size] "Set new cluster size") ;; this is event for cluster
  (set-payload [this payload] "Set new payload for this node") ;; and announce payload change event to cluster
  (set-restart-counter [this new-value] "Set restart-counter to particular value")
  (inc-tx [this] "Add 1 to current tx value")
  (upsert-neighbour [this neighbour-node] "Update existing or insert new neighbour to neighbour table")
  (delete-neighbour [this neighbour-id] "Delete neighbour from neighbour table")
  (set-event-queue [this new-event-queue] "Set new queue value for outgoing events")
  (put-event [this prepared-event] "Put prepared outgoing event to queue (FIFO)") ;; check neighbour :tx and if it's lower then put it to queue
  (take-event [this] "Take one prepared outgoing event from queue (FIFO)")
  (take-events [this n] "Take `n` prepared outgoing events from queue (FIFO)") ;; the group-by [:id :restart-counter :tx] to send the latest events only
  (upsert-ping [this ping-event] "Update existing or insert new active ping event in map")
  (delete-ping [this neighbour-id] "Delete active ping event from map")

  ;; Commands
  (start [this node-process-fn udp-dispatcher-fn] "Start this node and use `node-process-fn` as main node process and `udp-dispatcher-fn` to process incoming UDP packets")
  (stop [this] "Stop the node and leave the cluster")
  (join [this cb-fn] "Join this node to the cluster")
  (leave [this] "Leave the cluster")
  (ping [this neighbour-id] "Send Ping event to neighbour node") ;; NB: if in Ack id is different, then send event and change id in a neighbours table
  (probe [this host port] "Probe other node and if its alive then put it to a neighbours table")
  (ack [this ping-event] "Send Ack event to neighbour node"))


;; TODO:
;; How to clean neighbour table from old nodes?
;;

(defrecord NodeObject [*node]

           ISwimNode

           (value [^NodeObject this] @(:*node this))
           (id [^NodeObject this] (:id (.value this)))
           (host [^NodeObject this] (:host (.value this)))
           (port [^NodeObject this] (:port (.value this)))
           (restart-counter [^NodeObject this] (:restart-counter (.value this)))
           (tx [^NodeObject this] (:tx (.value this)))
           (cluster [^NodeObject this] (:cluster (.value this)))
           (cluster-size [^NodeObject this] (:cluster-size (:cluster (.value this))))
           (payload [^NodeObject this] (:payload (.value this)))
           (neighbour [^NodeObject this id] (get (:neighbours (.value this)) id))
           (neighbours [^NodeObject this] (:neighbours (.value this)))
           (status [^NodeObject this] (:status (.value this)))
           (event-queue [^NodeObject this] (:event-queue (.value this)))
           (ping-event [^NodeObject this neighbour-id] (get (:ping-events (.value this)) neighbour-id))
           (ping-events [^NodeObject this] (:ping-events (.value this)))

           (set-cluster [^NodeObject this cluster]
             (cond
               (not (s/valid? ::cluster cluster)) (throw (ex-info "Invalid cluster data" (->> cluster (s/explain-data ::cluster) spec-problems)))
               (not= :stop (.status this)) (throw (ex-info "Node is not stopped. Can't set new cluster value." {:current-status (.status this)}))
               :else (do
                       (d> :set-cluster (.id this) {:cluster (assoc cluster :secret-token "***censored***" :secret-key ["***censored***"])})
                       (swap! (:*node this) assoc :cluster cluster))))

           (set-cluster-size [^NodeObject this new-cluster-size]
             (when-not (s/valid? ::cluster-size new-cluster-size)
               (throw (ex-info "Invalid cluster size" (->> new-cluster-size (s/explain-data ::cluster-size) spec-problems))))
             (d> :set-cluster-size (.id this) {:new-cluster-size new-cluster-size})
             (swap! (:*node this) assoc :cluster (assoc (.cluster this) :cluster-size new-cluster-size)))

           (set-status [^NodeObject this new-status]
             (when-not (s/valid? ::status new-status)
               (throw (ex-info "Invalid node status" (->> new-status (s/explain-data ::status) spec-problems))))
             (d> :set-status (.id this) {:new-status new-status})
             (swap! (:*node this) assoc :status new-status))

           (inc-tx [^NodeObject this]
             (swap! (:*node this) assoc :tx (inc (.tx this))))

           (set-payload [^NodeObject this payload]
             ;;TODO: send event to cluster about new payload
             (d> :set-payload (.id this) {:payload payload})
             (swap! (:*node this) assoc :payload payload))

           (set-restart-counter [^NodeObject this restart-counter]
             (when-not (s/valid? ::restart-counter restart-counter)
               (throw (ex-info "Invalid restart counter data" (->> restart-counter (s/explain-data ::restart-counter) spec-problems))))
             (d> :set-restart-counter (.id this) {:restart-counter restart-counter})
             (swap! (:*node this) assoc :restart-counter restart-counter))

           (upsert-neighbour [^NodeObject this neighbour-node]
             (when-not (s/valid? ::neighbour-node neighbour-node)
               (throw (ex-info "Invalid neighbour node data" (->> neighbour-node (s/explain-data ::neighbour-node) spec-problems))))
             (d> :upsert-neighbour (.id this) {:neighbour-node neighbour-node})
             (swap! (:*node this) assoc :neighbours (assoc (neighbours this) (.-id ^NeighbourNode neighbour-node) (assoc neighbour-node :updated-at (System/currentTimeMillis)))))

           (delete-neighbour [^NodeObject this neighbour-id]
             (d> :delete-neighbour (.id this) {:neighbour-id neighbour-id})
             (swap! (:*node this) assoc :neighbours (dissoc (neighbours this) neighbour-id)))

           (set-event-queue [^NodeObject this new-event-queue]
             (when-not (s/valid? ::event-queue new-event-queue)
               (throw (ex-info "Invalid event queue data" (->> new-event-queue (s/explain-data ::event-queue) spec-problems))))
             (d> :set-event-queue (.id this) {:event-queue new-event-queue})
             (swap! (:*node this) assoc :event-queue new-event-queue))

           (put-event [^NodeObject this prepared-event]
             (when-not (vector? prepared-event)
               (throw (ex-info "Event should be a vector (prepared event)" {:prepared-event prepared-event})))
             (d> :put-event (.id this) {:prepared-event prepared-event :tx (inc (tx this))})
             (swap! (:*node this) assoc :event-queue (conj (.event_queue this) prepared-event) :tx (inc (tx this))))

           (take-event [^NodeObject this]
             (let [event (first (.event_queue this))]
               (swap! (:*node this) assoc :event-queue (->> this .event_queue rest vec))
               event))

           (take-events [^NodeObject this n]
             (let [events (->> this .event_queue (take n) vec)]
               (swap! (:*node this) assoc :event-queue (->> this .event_queue (drop n) vec))
               events))

           (upsert-ping [^NodeObject this ping-event]
             (when-not (s/valid? ::ping-event ping-event)
               (throw (ex-info "Invalid ping event data" (->> ping-event (s/explain-data ::ping-event) spec-problems))))
             (d> :upsert-ping (.id this) {:ping-event ping-event})
             (swap! (:*node this) assoc :ping-events (assoc (ping-events this) (:neighbour-id ping-event) ping-event)))

           (delete-ping [^NodeObject this neighbour-id]
             (d> :delete-ping (.id this) {:neighbour-id neighbour-id})
             (swap! (:*node this) assoc :ping-events (dissoc (.ping_events this) neighbour-id)))


           ;; NB: `node-process-fn` is a fn with one arg - [this], `udp-dispatcher-fn` is fn with two args: [this, udp-received-data]
           ;; `node-process-fn` looks for :continue? flag in UDP server. If it's false then `node-process-fn` terminates.
           (start [^NodeObject this node-process-fn udp-dispatcher-fn]
             (when (= (status this) :stop)
               (set-status this :left)
               (set-restart-counter this (inc (restart-counter this)))
               (let [{:keys [host port]} (value this)]
                 (swap! (:*node this) assoc :*udp-server (udp/start host port (partial udp-dispatcher-fn this))))
               (when-not (s/valid? ::node (.value this))
                 (throw (ex-info "Invalid node data" (->> this :*node (s/explain-data ::node) spec-problems))))
               (common/vfuture (node-process-fn this))
               (d> :start (.id this) {})))

           (leave [^NodeObject this]
             ;;TODO
             )

           (probe [^NodeObject this host port]
             )

           (stop [^NodeObject this]
             (let [{:keys [*udp-server scheduler-pool]} (value this)]
               (.leave this)
               (scheduler/stop-and-reset-pool! scheduler-pool :strategy :kill)
               (swap! (:*node this) assoc
                 :*udp-server (udp/stop *udp-server)
                 :ping-events {}
                 :event-queue []
                 :ping-round-buffer []
                 :tx 0)
               (set-status this :stop)
               (when-not (s/valid? ::node (.value this))
                 (throw (ex-info "Invalid node data" (spec-problems (s/explain-data ::node (:*node this)))))))
             (d> :stop (.id this) {})))


(defn new-node-object
  "Returns new NodeObject instance."

  (^NodeObject [node-data]
    (when-not (s/valid? ::node node-data)
      (throw (ex-info "Invalid node data" (spec-problems (s/explain-data ::node node-data)))))
    (d> :new-node-object (:id node-data) {:node-data (select-keys node-data [:host :port :status :restart-counter :tx])})
    (map->NodeObject {:*node (atom node-data)}))

  (^NodeObject [{:keys [^UUID id ^String host ^long port ^long restart-counter]} ^Cluster cluster]
    (new-node-object {:id                (or id (random-uuid))
                      :host              host
                      :port              port
                      :cluster           cluster
                      :status            :stop
                      :neighbours        {}
                      :restart-counter   (or restart-counter 0)
                      :tx                0
                      :ping-events       {}                  ;; active pings on the fly. we wait ack for them. key ::neighbour-id
                      :event-queue       []                  ;; outgoing events that we'll send to random logN neighbours next time
                      :ping-round-buffer []                  ;; we take logN neighbour ids to send events from event queue
                      :payload           {}                  ;; data that this node claims in cluster about itself
                      :scheduler-pool    (scheduler/mk-pool)
                      :*udp-server       nil})))


;;;;;;;;;;
;; Events
;;;;;;;;;;

(defprotocol ISwimEvent
  (prepare [this] "Convert Event to vector of values for subsequent serialization")
  (restore [this v] "Restore Event from vector of values"))


;;;;

(defrecord PingEvent [cmd-type id host port restart-counter tx neighbour-id attempt-number]

           ISwimEvent

           (prepare [^PingEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e) (.-neighbour_id e) (.-attempt_number e)])

           (restore [^PingEvent _ v]
             (if (and
                   (vector? v)
                   (= 8 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ping event-code)) uuid? string? pos-int? nat-int? nat-int? uuid? pos-int?] v)))
               (apply ->PingEvent v)
               (throw (ex-info "PingEvent vector has invalid structure" {:ping-vec v})))))


(defn new-ping
  ^PingEvent [^NodeObject n ^UUID neighbour-id attempt-number]
  (let [ping-event (map->PingEvent {:cmd-type        (:ping event-code)
                                    :id              (.id n)
                                    :host            (.host n)
                                    :port            (.port n)
                                    :restart-counter (.restart_counter n)
                                    :tx              (.tx n)
                                    :neighbour-id    neighbour-id
                                    :attempt-number  attempt-number})]
    (if-not (s/valid? ::ping-event ping-event)
      (throw (ex-info "Invalid ping event" (spec-problems (s/explain-data ::ping-event ping-event))))
      ping-event)))


(defn empty-ping
  ^PingEvent []
  (map->PingEvent {:cmd-type        (:ping event-code)
                   :id              (UUID. 0 0)
                   :host            "localhost"
                   :port            0
                   :restart-counter 0
                   :tx              0
                   :neighbour-id    (UUID. 0 0)
                   :attempt-number  1}))


;;;;


(defrecord AckEvent [cmd-type id restart-counter tx neighbour-id neighbour-tx]

           ISwimEvent

           (prepare [^AckEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e) (.-neighbour_tx e)])

           (restore [^AckEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ack event-code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
               (apply ->AckEvent v)
               (throw (ex-info "AckEvent vector has invalid structure" {:ack-vec v})))))


(defn new-ack
  ^AckEvent [^NodeObject n ^PingEvent e]
  (let [ack-event (map->AckEvent {:cmd-type        (:ack event-code)
                                  :id              (.id n)
                                  :restart-counter (.restart_counter n)
                                  :tx              (.tx n)
                                  :neighbour-id    (.-id e)
                                  :neighbour-tx    (.-tx e)})]
    (if-not (s/valid? ::ack-event ack-event)
      (throw (ex-info "Invalid ack event" (spec-problems (s/explain-data ::ack-event ack-event))))
      ack-event)))



(defn empty-ack
  ^AckEvent []
  (map->AckEvent {:cmd-type        (:ack event-code)
                  :id              (UUID. 0 0)
                  :restart-counter 0
                  :tx              0
                  :neighbour-id    (UUID. 0 0)
                  :neighbour-tx    0}))


;;;;;

(defrecord DeadEvent [cmd-type id restart-counter tx neighbour-id neighbour-tx]

           ISwimEvent

           (prepare [^DeadEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e) (.-neighbour_tx e)])

           (restore [^DeadEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:dead event-code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
               (apply ->DeadEvent v)
               (throw (ex-info "DeadEvent vector has invalid structure" {:dead-vec v})))))


(defn new-dead
  ^DeadEvent [^NodeObject this ^PingEvent e]
  (let [dead-event (map->DeadEvent {:cmd-type        (:dead event-code)
                                    :id              (.id this)
                                    :restart-counter (.restart_counter this)
                                    :tx              (.tx this)
                                    :neighbour-id    (.-id e)
                                    :neighbour-tx    (.-tx e)})]
    (if-not (s/valid? ::dead-event dead-event)
      (throw (ex-info "Invalid dead event" (spec-problems (s/explain-data ::dead-event dead-event))))
      dead-event)))



(defn empty-dead
  ^DeadEvent []
  (map->DeadEvent {:cmd-type        (:dead event-code)
                   :id              (UUID. 0 0)
                   :restart-counter 0
                   :tx              0
                   :neighbour-id    (UUID. 0 0)
                   :neighbour-tx    0}))


;;;;

(defrecord ProbeEvent [cmd-type id host port restart-counter tx neighbour-host neighbour-port]

           ISwimEvent

           (prepare [^ProbeEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e) (.-neighbour_host e) (.-neighbour_port e)])

           (restore [^ProbeEvent _ v]
             (if (and
                   (vector? v)
                   (= 8 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:probe event-code)) uuid? string? pos-int? nat-int? nat-int? string? pos-int?] v)))
               (apply ->ProbeEvent v)
               (throw (ex-info "ProbeEvent vector has invalid structure" {:probe-vec v})))))


(defn new-probe
  ^ProbeEvent [^NodeObject n ^String neighbour-host neighbour-port]
  (let [probe-event (map->ProbeEvent {:cmd-type        (:probe event-code)
                                      :id              (.id n)
                                      :host            (.host n)
                                      :port            (.port n)
                                      :restart-counter (.restart_counter n)
                                      :tx              (.tx n)
                                      :neighbour-host  neighbour-host
                                      :neighbour-port  neighbour-port})]
    (if-not (s/valid? ::probe-event probe-event)
      (throw (ex-info "Invalid probe event" (spec-problems (s/explain-data ::probe-event probe-event))))
      probe-event)))


(defn empty-probe
  ^ProbeEvent []
  (map->ProbeEvent {:cmd-type        (:probe event-code)
                    :id              (UUID. 0 0)
                    :host            "localhost"
                    :port            0
                    :restart-counter 0
                    :tx              0
                    :neighbour-host  "localhost"
                    :neighbour-port  0}))


;;;;


(defrecord ProbeAckEvent [cmd-type id restart-counter tx neighbour-id neighbour-tx]

           ISwimEvent

           (prepare [^ProbeAckEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e) (.-neighbour_tx e)])

           (restore [^ProbeAckEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:probe-ack event-code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
               (apply ->ProbeAckEvent v)
               (throw (ex-info "ProbeAckEvent vector has invalid structure" {:probe-ack-vec v})))))


(defn new-probe-ack
  ^ProbeAckEvent [^NodeObject n ^ProbeEvent e]
  (let [ack-event (map->ProbeAckEvent {:cmd-type        (:probe-ack event-code)
                                       :id              (.id n)
                                       :restart-counter (.restart_counter n)
                                       :tx              (.tx n)
                                       :neighbour-id    (.-id e)
                                       :neighbour-tx    (.-tx e)})]
    (if-not (s/valid? ::probe-ack-event ack-event)
      (throw (ex-info "Invalid probe ack event" (spec-problems (s/explain-data ::probe-ack-event ack-event))))
      ack-event)))



(defn empty-probe-ack
  ^ProbeAckEvent []
  (map->ProbeAckEvent {:cmd-type        (:probe-ack event-code)
                       :id              (UUID. 0 0)
                       :restart-counter 0
                       :tx              0
                       :neighbour-id    (UUID. 0 0)
                       :neighbour-tx    0}))



;;;;

(defn build-anti-entropy-data
  "Build anti-entropy data – subset of known nodes from neighbours map.
  This data is propagated from node to node and thus nodes can get knowledge about unknown hosts.
  To apply anti-entropy data receiver should compare incarnation pair [restart-counter tx] and apply only
  if node has older data.
  Returns vector of known neighbors size of `num` if any or empty vector."
  [^NodeObject this & {:keys [num] :or {num 2}}]
  (or
    (some->>
      (.neighbours this)
      vals
      shuffle
      (take num)
      (map #(into {} %))
      vec)
    []))


(defrecord AntiEntropy [cmd-type anti-entropy-data]
           ISwimEvent

           (prepare [^AntiEntropy e]
             [(.-cmd_type e) (.-anti_entropy_data e)])

           (restore [^AntiEntropy _ v]
             (if (and
                   (vector? v)
                   (= 2 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:anti-entropy event-code)) vector?] v)))
               (apply ->AntiEntropy v)
               (throw (ex-info "AntiEntropy vector has invalid structure" {:anti-entropy-vec v})))))


(defn new-anti-entropy
  ^AntiEntropy [^NodeObject this]
  (let [anti-entropy-data (build-anti-entropy-data this)
        ae-event          (map->AntiEntropy {:cmd-type          (:anti-entropy event-code)
                                             :anti-entropy-data anti-entropy-data})]
    (if-not (s/valid? ::anti-entropy-event ae-event)
      (throw (ex-info "Invalid anti-entropy event" (spec-problems (s/explain-data ::anti-entropy-event ae-event))))
      ae-event)))


(defn empty-anti-entropy
  ^AntiEntropy []
  (map->AntiEntropy {:cmd-type          (:anti-entropy event-code)
                     :anti-entropy-data []}))


;;;;;;;;;;


(defn node-process-fn
  [^NodeObject this]
  (let [*idx (atom 0)
        rot  ["\\" "|" "/" "—"]]
    (Thread/sleep 100)
    (while (-> this .value :*udp-server deref :continue?)
      (print (format "\rNode is active: %s  " (nth rot (rem @*idx (count rot)))))
      (Thread/sleep 200)
      (swap! *idx inc)
      (flush)))
  (println "Node is stopped."))


;;;;

(defmulti restore-event (fn [x] (.get ^PersistentVector x 0)))

(defmethod restore-event 0 ^PingEvent [e] (.restore (empty-ping) e))
(defmethod restore-event 1 ^AckEvent [e] (.restore (empty-dead) e))
(defmethod restore-event 8 ^AntiEntropy [e] (.restore (empty-anti-entropy) e))
(defmethod restore-event 9 ^ProbeEvent [e] (.restore (empty-probe) e))


;;;;


(defn suitable-restart-counter?
  "Check that restart counter from neighbours map is less or equal than from event.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this e]
  (let [neighbour-id (:id e)]
    (when-let [nb ^NeighbourNode (.neighbour this neighbour-id)]
      (<= (.-restart_counter nb) (:restart-counter e)))))


(defn suitable-tx?
  "Check that tx from neighbours map is less or equal than from event.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this e]
  (let [neighbour-id (:id e)]
    (when-let [nb ^NeighbourNode (.neighbour this neighbour-id)]
      (<= (.-tx nb) (:tx e)))))


(defn suitable-incarnation?
  "Check that incarnation, pair [restart-counter tx] from neighbours map is less or equal than from event.
  Returns true, if suitable and false if not."
  [^NodeObject this e]
  (= [true true] [(suitable-restart-counter? this e) (suitable-tx? this e)]))


;;;;;;;;;;

(defn build-anti-entropy-data
  "Build anti-entropy data – subset of known nodes from neighbours map.
  This data is propagated from node to node and thus nodes can get knowledge about unknown hosts.
  To apply anti-entropy data receiver should compare incarnation pair [restart-counter tx] and apply only
  if node has older data.
  Returns vector of known neighbors size of `num`."
  [^NodeObject this & {:keys [num] :or {num 2}}]
  (->>
    (.neighbours this)
    vals
    shuffle
    (take num)
    vec))


(defmulti process-incoming-event (fn [this e] (type e)))


(defmethod process-incoming-event ProbeEvent
  [^NodeObject this ^ProbeEvent e]

  (when (not (.neighbour this (.-id e)))
    (let [new-neighbour (new-neighbour-node {:id              (.-id e)
                                             :host            (.-host e)
                                             :port            (.-port e)
                                             :status          :left
                                             :access          :direct
                                             :restart-counter (.-restart_counter e)
                                             :tx              (.-tx e)
                                             :payload         {}
                                             :updated-at      (System/currentTimeMillis)})]

      (d> :process-incoming-event-probe-add-new-neighbour (.id this) new-neighbour)
      (upsert-neighbour this new-neighbour)))

  (.inc_tx this)                                            ;; every event on node increments tx
  (send-event-with-anti-entropy this (new-probe-ack this e) (.-host e) (.-port e)))


(defmethod process-incoming-event PingEvent
  [^NodeObject this ^PingEvent e]

  #_(cond
      (not (suitable-restart-counter? this e)) ())
  ;; проверить restart counter у соседа и если он меньше чем нам известно, то ответить что узел нуждается в перезагрузке
  ;; прекратить дальнейшую обработку
  ;; проверить, знаем ли мы такого соседа. Если нет, то внести в таблицу соседей.
  ;; проверить, что tx из event у neighbour больше или равен чем известный нам в таблице соседей.
  ;; если меньше, то это событие из прошлого и надо игнорировать
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
  (.neighbours this)
  (println (.tx this)))


(defmethod process-incoming-event :default
  [^NodeObject this e]
  (d> :process-incoming-event-default (.id this) {:msg "Unknown event type" :event e}))


;;;;

(defn udp-dispatcher-fn
  [^NodeObject this ^bytes udp-data]
  (let [secret-key     (-> this .cluster :secret-key)
        decrypted-data (safe (e/decrypt-data ^bytes secret-key ^bytes udp-data)) ;; Ignore bad messages
        events-vector  (deserialize ^bytes decrypted-data)]
    (if (vector? events-vector)
      (doseq [serialized-event events-vector]
        (let [event (restore-event serialized-event)]
          (.inc_tx this)                                    ;; Every incoming event must increment tx
          (d> :process-incoming-event (.id this) {:event event})
          (process-incoming-event this event)))
      (d> :udp-dispatcher-fn (.id this) {:msg "Bad events vector structure" :events-vector events-vector}))))




