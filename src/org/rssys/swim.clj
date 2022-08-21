(ns org.rssys.swim
  "SWIM functions, specs and domain entities"
  (:require
    [clojure.spec.alpha :as s]
    [cognitect.transit :as transit]
    [org.rssys.encrypt :as e]
    [org.rssys.scheduler :as scheduler]
    [org.rssys.udp :as udp])
  (:import
    (java.io
      ByteArrayInputStream
      ByteArrayOutputStream
      Writer)
    (java.util
      UUID)))


;;;;;;;;;;;;;;;;
;; SWIM spec
;;;;;;;;;;;;;;;;

(s/def ::id uuid?)
(s/def ::neighbour-id ::id)
(s/def ::host string?)
(s/def ::port (s/and pos-int? #(< % 65536)))
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
(s/def ::payload ::object)                                  ;; some data attached to node
(s/def ::updated-at nat-int?)
(s/def ::neighbour-node (s/keys :req-un [::id ::host ::port ::status ::access ::restart-counter ::tx ::payload ::updated-at]))
(s/def ::neighbours (s/map-of ::neighbour-id ::neighbour-node))

(s/def ::attempt-number pos-int?)
(s/def ::ping-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::neighbour-id ::attempt-number]))
(s/def ::ping-events (s/map-of ::neighbour-id ::ping-event))

(s/def ::neighbour-tx ::tx)
(s/def ::ack-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::neighbour-id ::neighbour-tx]))

(s/def ::scheduler-pool ::object)
(s/def ::*udp-server ::object)
(s/def ::event-queue vector?)
(s/def ::ping-round-buffer (s/coll-of ::neighbour-id))


(s/def ::node
  (s/keys :req-un [::id ::host ::port ::cluster ::status ::neighbours ::restart-counter
                   ::tx ::ping-events ::payload ::scheduler-pool ::*udp-server ::event-queue
                   ::ping-round-buffer]))


(def event-code
  {:ping 0 :ack 1 :join 2 :alive 3 :suspect 4 :left 5 :dead 6 :payload 7 :anti-entropy 8})


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
  (let [in     (ByteArrayInputStream. barray)
        reader (transit/reader in :msgpack)]
    (transit/read reader)))


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

  (^NeighbourNode [nn]
    (if-not (s/valid? ::neighbour-node nn)
      (throw (ex-info "Invalid neighbour data" (->> nn (s/explain-data ::neighbour-node) spec-problems)))
      (map->NeighbourNode nn)))


  (^NeighbourNode [^String host ^long port]
    (new-neighbour-node {:id              (UUID. 0 0)
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
  (restart-counter [this] "Get node restart counter")
  (tx [this] "Get node tx")
  (cluster [this] "Get cluster value")
  (cluster-size [this] "Get cluster size")
  (payload [this] "Get node payload")
  (neighbours [this] "Get node neighbours")
  (status [this] "Get current node status")
  (event-queue [this] "Get vector of prepared events")
  (ping-event [this neighbour-id] "Get ping event by neighbour id if exist")
  (ping-events [this] "Get map of active ping events")

  ;; Setters
  (set-cluster [this cluster] "Set new cluster for this node")
  (set-cluster-size [this new-cluster-size] "Set new cluster size") ;; this is event for cluster
  (set-payload [this payload] "Set new payload for this node") ;; and announce payload change event to cluster
  (set-restart-counter [this new-value] "Set restart-counter to particular value")
  (upsert-neighbour [this neighbour-node] "Update existing or insert new neighbour to neighbour table")
  (delete-neighbour [this neighbour-id] "Delete neighbour from neighbour table")
  (set-event-queue [this new-event-queue] "Set new event queue value")
  (put-event [this prepared-event] "Put prepared event to queue (FIFO)") ;; check neighbour :tx and if it's lower then put it to queue
  (take-event [this] "Take one prepared event from queue (FIFO)")
  (take-events [this n] "Take `n` prepared events from queue (FIFO)") ;; the group-by [:id :restart-counter :tx] to send the latest events only
  (upsert-ping [this ping-event] "Update existing or insert new ping event to a table")
  (delete-ping [this neighbour-id] "Delete ping event from table")

  ;; Commands
  (start [this process-cb-fn] "Start this node")
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
           (restart-counter [^NodeObject this] (:restart-counter (.value this)))
           (tx [^NodeObject this] (:tx (.value this)))
           (cluster [^NodeObject this] (:cluster (.value this)))
           (cluster-size [^NodeObject this] (:cluster-size (:cluster (.value this))))
           (payload [^NodeObject this] (:payload (.value this)))
           (neighbours [^NodeObject this] (:neighbours (.value this)))
           (status [^NodeObject this] (:status (.value this)))
           (event-queue [^NodeObject this] (:event-queue (.value this)))
           (ping-event [^NodeObject this neighbour-id] (get (:ping-events (.value this)) neighbour-id))
           (ping-events [^NodeObject this] (:ping-events (.value this)))

           (set-cluster [^NodeObject this cluster]
             (cond
               (not (s/valid? ::cluster cluster)) (throw (ex-info "Invalid cluster data" (->> cluster (s/explain-data ::cluster) spec-problems)))
               (not= :stop (.status this)) (throw (ex-info "Node is not stopped. Can't set new cluster value." {:current-status (.status this)}))
               :else (swap! (:*node this) assoc :cluster cluster)))

           (set-cluster-size [^NodeObject this new-cluster-size]
             (if-not (s/valid? ::cluster-size new-cluster-size)
               (throw (ex-info "Invalid cluster size" (->> new-cluster-size (s/explain-data ::cluster-size) spec-problems)))
               (swap! (:*node this) assoc :cluster (assoc (.cluster this) :cluster-size new-cluster-size))))

           (set-payload [^NodeObject this payload]
             ;;TODO: send event to cluster about new payload
             (swap! (:*node this) assoc :payload payload))

           (set-restart-counter [^NodeObject this restart-counter]
             (if-not (s/valid? ::restart-counter restart-counter)
               (throw (ex-info "Invalid restart counter data" (->> restart-counter (s/explain-data ::restart-counter) spec-problems)))
               (swap! (:*node this) assoc :restart-counter restart-counter)))

           (upsert-neighbour [^NodeObject this neighbour-node]
             (if-not (s/valid? ::neighbour-node neighbour-node)
               (throw (ex-info "Invalid neighbour node data" (->> neighbour-node (s/explain-data ::neighbour-node) spec-problems)))
               (swap! (:*node this) assoc :neighbours (assoc (neighbours this) (.-id ^NeighbourNode neighbour-node) (assoc neighbour-node :updated-at (System/currentTimeMillis))))))

           (delete-neighbour [^NodeObject this neighbour-id]
             (swap! (:*node this) assoc :neighbours (dissoc (neighbours this) neighbour-id)))

           (set-event-queue [^NodeObject this new-event-queue]
             (if-not (s/valid? ::event-queue new-event-queue)
               (throw (ex-info "Invalid event queue data" (->> new-event-queue (s/explain-data ::event-queue) spec-problems)))
               (swap! (:*node this) assoc :event-queue new-event-queue)))

           (put-event [^NodeObject this prepared-event]
             (if (vector? prepared-event)
               (swap! (:*node this) assoc :event-queue (conj (.event_queue this) prepared-event)
                 :tx (inc (tx this)))
               (throw (ex-info "Event should be a vector (prepared event)" {:prepared-event prepared-event}))))

           (take-event [^NodeObject this]
             (let [event (first (.event_queue this))]
               (swap! (:*node this) assoc :event-queue (->> this .event_queue rest vec))
               event))

           (take-events [^NodeObject this n]
             (let [events (->> this .event_queue (take n) vec)]
               (swap! (:*node this) assoc :event-queue (->> this .event_queue (drop n) vec))
               events))

           (upsert-ping [^NodeObject this ping-event]
             (if-not (s/valid? ::ping-event ping-event)
               (throw (ex-info "Invalid ping event data" (->> ping-event (s/explain-data ::ping-event) spec-problems)))
               (swap! (:*node this) assoc :ping-events (assoc (ping-events this) (:neighbour-id ping-event) ping-event))))

           (delete-ping [^NodeObject this neighbour-id]
             (swap! (:*node this) assoc :ping-events (dissoc (.ping_events this) neighbour-id)))

           (start [^NodeObject this cb-fn]
             (let [{:keys [host port restart-counter]} (.value this)]
               (swap! (:*node this) assoc
                 :*udp-server (udp/start host port cb-fn)
                 :status :left
                 :restart-counter restart-counter)
               (when-not (s/valid? ::node (.value this))
                 (throw (ex-info "Invalid node data" (->> this :*node (s/explain-data ::node) spec-problems))))))

           (leave [^NodeObject this]
             ;;TODO
             )

           (stop [^NodeObject this]
             (let [{:keys [*udp-server restart-counter scheduler-pool]} (value this)]
               (.leave this)
               (scheduler/stop-and-reset-pool! scheduler-pool :strategy :kill)
               (swap! (:*node this) assoc
                 :*udp-server (udp/stop *udp-server)
                 :status :stop
                 :restart-counter (inc restart-counter)
                 :ping-events []
                 :tx 0)
               (when-not (s/valid? ::node (.value this))
                 (throw (ex-info "Invalid node data" (spec-problems (s/explain-data ::node (:*node this)))))))))


(defn new-node-object
  "Returns new NodeObject instance."

  (^NodeObject [node-data]
    (if-not (s/valid? ::node node-data)
      (throw (ex-info "Invalid node data" (spec-problems (s/explain-data ::node node-data))))
      (map->NodeObject {:*node (atom node-data)})))

  (^NodeObject [{:keys [^UUID id ^String host ^long port ^long restart-counter]} ^Cluster cluster]
    (new-node-object {:id                (or id (random-uuid))
                      :host              host
                      :port              port
                      :cluster           cluster
                      :status            :stop
                      :neighbours        {}
                      :restart-counter   (or restart-counter 0)
                      :tx                0
                      :ping-events       {}                  ;; pings on the fly. we wait ack for them. key ::neighbour-id
                      :event-queue       []                  ;; events that we'll send to random logN neighbours next time
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

(defrecord PingEvent [cmd-type id restart-counter tx neighbour-id attempt-number]

           ISwimEvent

           (prepare [^PingEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.-tx e) (.-neighbour_id e) (.-attempt_number e)])

           (restore [^PingEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ping event-code)) uuid? nat-int? nat-int? uuid? pos-int?] v)))
               (apply ->PingEvent v)
               (throw (ex-info "PingEvent vector has invalid structure" {:ping-vec v})))))


(defn new-ping
  ^PingEvent [^NodeObject n ^UUID neighbour-id attempt-number]
  (let [ping-event (map->PingEvent {:cmd-type        (:ping event-code)
                                    :id              (.id n)
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

