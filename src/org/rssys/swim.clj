(ns org.rssys.swim
  "SWIM functions, specs and domain entities"
  (:require
    [clojure.spec.alpha :as s]
    [cognitect.transit :as transit]
    [org.rssys.scheduler :as scheduler]
    [org.rssys.udp :as udp])
  (:import
    (clojure.lang
      PersistentVector)
    (java.io
      ByteArrayInputStream
      ByteArrayOutputStream
      Writer)
    (java.security
      SecureRandom)
    (java.util
      UUID)
    (javax.crypto
      Cipher
      SecretKeyFactory)
    (javax.crypto.spec
      GCMParameterSpec
      IvParameterSpec
      PBEKeySpec
      SecretKeySpec)))


;;;;;;;;;;;;;;;;
;; SWIM spec
;;;;;;;;;;;;;;;;

(s/def ::id uuid?)
(s/def ::neighbour-id ::id)
(s/def ::host string?)
(s/def ::port (s/and pos-int? #(< % 65536)))
(s/def ::name string?)
(s/def ::desc string?)
(s/def ::status #{:stopped :joining :normal :suspicious :leave :dead :unknown})
(s/def ::access #{:direct :indirect})
(s/def ::object any?)
(s/def ::tags set?)                                         ;; #{"dc1" "test"}
(s/def ::nspace (s/or :symbol symbol? :keyword keyword? :string string?)) ;; cluster namespace
(s/def ::secret-token string?)
(s/def ::secret-key ::object)                               ;; 256-bit SecretKey
(s/def ::cluster (s/keys :req-un [::id ::name ::desc ::secret-token ::nspace ::tags] :opt-un [::secret-key]))

(s/def ::restart-counter nat-int?)
(s/def ::tx nat-int?)
(s/def ::payload ::object)                                  ;; some data attached to node
(s/def ::neighbour-node (s/keys :req-un [::id ::host ::port ::status ::access ::restart-counter ::tx ::payload]))
(s/def ::neighbours (s/map-of ::neighbour-id ::neighbour-node))

(s/def ::ping-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::neighbour-id]))
(s/def ::ping-events (s/coll-of ::ping-event))

(s/def ::scheduler-pool ::object)
(s/def ::*udp-server ::object)


(s/def ::node
  (s/keys :req-un [::id ::host ::port ::cluster ::status ::neighbours ::restart-counter
                   ::tx ::ping-events ::payload ::scheduler-pool ::*udp-server]))


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


;;;;;;;;;;


(defn new-iv-12
  "Create new random init vector using SecureRandom.
  Returns byte array 12 bytes length with random data."
  []
  (let [iv-array (byte-array 12)]
    (.nextBytes (SecureRandom.) iv-array)
    iv-array))


(defn gen-secret-key
  "Generate secret key based on a given token string.
  Returns bytes array 256-bit length."
  [^String secret-token]
  (let [salt       (.getBytes "org.rssys.password.salt.string!!")
        factory    (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA256")
        spec       (PBEKeySpec. (.toCharArray secret-token) salt 10000 256)
        secret-key (.getEncoded (.generateSecret factory spec))]
    secret-key))


(defn init-cipher
  "Init cipher using given secret-key (32 bytes) and IV (12 bytes).
  Cipher mode may be :encrypt or :decrypt
  Returns ^Cipher."
  [^bytes secret-key cipher-mode ^bytes iv-bytes]
  (let [cipher             (Cipher/getInstance "AES/GCM/NoPadding")
        gcm-tag-length-bit 128
        gcm-iv-spec        (GCMParameterSpec. gcm-tag-length-bit iv-bytes)
        cipher-mode-value  ^long (cond
                                   (= :encrypt cipher-mode) Cipher/ENCRYPT_MODE
                                   (= :decrypt cipher-mode) Cipher/DECRYPT_MODE
                                   :else (throw (ex-info "Wrong cipher mode" {:cipher-mode cipher-mode})))]
    (.init cipher cipher-mode-value (SecretKeySpec. secret-key "AES") gcm-iv-spec)
    cipher))


(defn encrypt-bytes
  "Encrypt plain data using given initialized ^Cipher in encryption mode.
   Returns encrypted bytes array."
  ^bytes
  [^Cipher cipher ^bytes plain-bytes]
  (.doFinal cipher plain-bytes))


(defn decrypt-bytes
  "Decrypt data using given initialized ^Cipher in decryption mode.
  Returns plain data bytes array."
  ^bytes
  [^Cipher cipher ^bytes encrypted-bytes]
  (.doFinal cipher encrypted-bytes))


;;;;;;;;;;;;;;;;;;;
;; Domain entities
;;;;;;;;;;;;;;;;;;;

(defn cluster-str
  "Returns String representation of Cluster"
  ^String
  [cluster]
  (str (into {} (assoc cluster :secret-token "***censored***"))))


(defrecord Cluster [id name desc secret-token nspace tags secret-key]
           Object
           (toString [this] (cluster-str this)))


(defn new-cluster
  "Returns new Cluster instance."
  ^Cluster [{:keys [id name desc secret-token nspace tags] :as c}]
  (when-not (s/valid? ::cluster c)
    (throw (ex-info "Invalid cluster data" (spec-problems (s/explain-data ::cluster c)))))
  (map->Cluster {:id     (or id (random-uuid)) :name name :desc desc :secret-token secret-token
                 :nspace nspace :tags tags :secret-key (gen-secret-key secret-token)}))


(defmethod print-method Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


(defmethod print-dup Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


;;;;;;;;;


(defrecord NeighbourNode [id host port status access restart-counter tx payload])


(defn new-neighbour-node
  "Returns new NeighbourNode instance."

  (^NeighbourNode [nn]
    (if-not (s/valid? ::neighbour-node nn)
      (throw (ex-info "Invalid neighbour data" (spec-problems (s/explain-data ::neighbour-node nn))))
      (map->NeighbourNode nn)))


  (^NeighbourNode [^String host ^long port]
    (new-neighbour-node {:id              (UUID. 0 0)
                         :host            host
                         :port            port
                         :status          :unknown
                         :access          :direct
                         :restart-counter 0
                         :tx              0 :payload {}})))



(defrecord Node [id host port cluster status neighbours restart-counter tx ping-events
                 payload scheduler-pool *udp-server]
           Object
           (toString [this] (.toString this)))



(defprotocol ISwimNode
  "SWIM Node protocol"
  :extend-via-metadata true

  ;; Getters
  (value [this] "Get node state value")
  (id [this] "Get node id")
  (cluster [this] "Get cluster value")
  (payload [this] "Get payload value")
  (neighbours [this] "Get neighbours")
  (status [this] "Get current value of node status")

  ;; Setters
  (set-cluster [this cluster] "Set new cluster")
  (set-payload [this payload] "Set new payload for this node")
  (set-restart-counter [this new-value] "Set restart-counter to particular value")
  (add-neighbour [this neighbour-node] "Add new neighbour to neighbour table")
  (delete-neighbour [this neighbour-id] "Remove neighbour from neighbour table")

  ;; Commands
  (start [this process-cb-fn] "Start this node")
  (stop [this] "Stop the node and leave the cluster")
  (join [this cb-fn] "Join this node to the cluster")
  (leave [this] "Leave the cluster")
  (ping [this neighbour-id] "Send Ping event to neighbour node") ;; NB: if in Ack id is different, then send event and change id in a neighbours table
  (probe [this host port] "Probe other node and if its alive then put it to a neighbours table")
  (ack [this ping-event] "Send Ack event to neighbour node"))


(defrecord NodeObject [*node]

           ISwimNode

           (value [^NodeObject this] @(:*node this))
           (id [^NodeObject this] (:id (.value this)))
           (cluster [^NodeObject this] (:cluster (.value this)))
           (payload [^NodeObject this] (:payload (.value this)))
           (neighbours [^NodeObject this] (:neighbours (.value this)))
           (status [^NodeObject this] (:status (.value this)))

           (set-cluster [^NodeObject this cluster]
             (cond
               (not (s/valid? ::cluster cluster)) (throw (ex-info "Invalid cluster data" (spec-problems (s/explain-data ::cluster cluster))))
               (not= :stopped (.status this)) (throw (ex-info "Node is not stopped. Can't set new cluster value." {:current-status (.status this)}))
               :else (swap! (:*node this) assoc :cluster cluster)))

           (set-payload [^NodeObject this payload]
             ;;TODO: send event to cluster about new payload
             (swap! (:*node this) assoc :payload payload))

           (set-restart-counter [^NodeObject this restart-counter]
             (if-not (s/valid? ::restart-counter restart-counter)
               (throw (ex-info "Invalid restart counter data" (spec-problems (s/explain-data ::restart-counter restart-counter))))
               (swap! (:*node this) assoc :restart-counter restart-counter)))

           (add-neighbour [^NodeObject this neighbour-node]
             (if-not (s/valid? ::neighbour-node neighbour-node)
               (throw (ex-info "Invalid neighbour node data" (spec-problems (s/explain-data ::neighbour-node neighbour-node))))
               (swap! (:*node this) assoc :neighbours (assoc (neighbours this) (:id neighbour-node) neighbour-node))))

           (delete-neighbour [^NodeObject this neighbour-id]
             (swap! (:*node this) assoc :neighbours (dissoc (neighbours this) neighbour-id)))

           (start [^NodeObject this cb-fn]
             (let [{:keys [host port restart-counter]} (.value this)]
               (swap! (:*node this) assoc
                 :*udp-server (udp/start host port cb-fn)
                 :status :leave
                 :restart-counter restart-counter)
               (when-not (s/valid? ::node (.value this))
                 (throw (ex-info "Invalid node data" (spec-problems (s/explain-data ::node (:*node this))))))))

           (leave [^NodeObject this]
             ;;TODO
             )

           (stop [^NodeObject this]
             (let [{:keys [*udp-server restart-counter scheduler-pool]} (value this)]
               (.leave this)
               (scheduler/stop-and-reset-pool! scheduler-pool :strategy :kill)
               (swap! (:*node this) assoc
                 :*udp-server (udp/stop *udp-server)
                 :status :stopped
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
    (new-node-object {:id              (or id (random-uuid))
                      :host            host
                      :port            port
                      :cluster         cluster
                      :status          :stopped
                      :neighbours      {}
                      :restart-counter (or restart-counter 0)
                      :tx              0
                      :ping-events     []
                      :payload         {}
                      :scheduler-pool  (scheduler/mk-pool)
                      :*udp-server     nil})))



;;;;;;;;;;;;
;;;; Events
;;
;;(def event-code
;;  {:ping 0 :ack 1 :joining 2 :normal 3 :suspicious 4 :leave 5 :dead 6 :payload 7 :anti-entropy 8})
;;
;;
;;(defprotocol ISwimEvent
;;  (prepare [this] "Convert Event to vector of values for subsequent serialization")
;;  (restore [this v] "Restore Event from vector of values (overwrites this)."))
;;
;;
;;(defrecord PingEvent [cmd-type id restart-counter tx-counter receiver-id]
;;
;;           ISwimEvent
;;
;;           (prepare [^PingEvent e]
;;             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.-tx_counter e) (.-receiver_id e)])
;;
;;           (restore [^PingEvent _ v]
;;             (if (and
;;                   (vector? v)
;;                   (= 5 (count v))
;;                   (every? true? (map #(%1 %2) [#(= % (:ping event-code)) uuid? nat-int? nat-int? uuid?] v)))
;;               (apply ->PingEvent v)
;;               (throw (ex-info "PingEvent vector has invalid structure" {:ping-vec v})))))
;;
;;
;;(defrecord AckEvent [cmd-type id restart-counter tx-counter receiver-id receiver-tx-counter]
;;
;;           ISwimEvent
;;
;;           (prepare [^AckEvent e]
;;             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.-tx_counter e) (.-receiver_id e) (.-receiver_tx_counter e)])
;;
;;           (restore [^AckEvent _ v]
;;             (if (and
;;                   (vector? v)
;;                   (= 6 (count v))
;;                   (every? true? (map #(%1 %2) [#(= % (:ack event-code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
;;               (apply ->AckEvent v)
;;               (throw (ex-info "AckEvent vector has invalid structure" {:ack-vec v})))))
;;
;;
;;(defrecord JoiningEvent [cmd-type id restart-counter tx-counter host port]
;;           ISwimEvent
;;           (prepare [^JoiningEvent e]
;;             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.-tx_counter e) (.-host e) (.-port e)]))
;;
;;
;;(defrecord NormalEvent [cmd-type id]
;;           ISwimEvent
;;           (prepare [^NormalEvent e]
;;             [(.-cmd_type e) (.-id e)]))
;;
;;
;;(defrecord SuspiciousEvent [cmd-type id]
;;           ISwimEvent
;;           (prepare [^SuspiciousEvent e]
;;             [(.-cmd_type e) (.-id e)]))
;;
;;
;;(defrecord LeaveEvent [cmd-type id]
;;           ISwimEvent
;;           (prepare [^LeaveEvent e]
;;             [(.-cmd_type e) (.-id e)]))
;;
;;
;;(defrecord DeadEvent [cmd-type id]
;;           ISwimEvent
;;           (prepare [^DeadEvent e]
;;             [(.-cmd_type e) (.-id e)]))
;;
;;
;;(defrecord PayloadEvent [cmd-type, id, data]
;;           ISwimEvent
;;           (prepare [^PayloadEvent e]
;;             [(.-cmd_type e) (.-id e) (.-data e)]))
;;
;;
;;(defrecord AntiEntropyEvent [cmd-type data]
;;           ISwimEvent
;;           (prepare [^AntiEntropyEvent e]
;;             [(.-cmd_type e) (.-data e)]))
;;
;;
;;(defn ^PingEvent new-ping
;;  [^Node n ^UUID receiver-id]
;;  (->PingEvent
;;    (:ping event-code) (:id n) (:restart-counter n) (:tx-counter n) receiver-id))
;;
;;
;;(defn ^PingEvent empty-ping
;;  []
;;  (->PingEvent
;;    (:ping event-code) (UUID. 0 0) 0 0 (UUID. 0 0)))
;;
;;
;;(defn ^PingEvent restore-ping
;;  "Resto"
;;  [v]
;;  (if (and
;;        (vector? v)
;;        (= 5 (count v))
;;        (every? true? (map #(%1 %2) [#(= % (:ping event-code)) uuid? nat-int? nat-int? uuid?] v)))
;;    (apply ->PingEvent v)
;;    (throw (ex-info "PingEvent vector has invalid structure" {:ping-vec v}))))
;;
;;
;;(defn new-ack
;;  [^Node n ^PingEvent e]
;;  (->AckEvent
;;    (:ack event-code) (:id n) (:restart-counter n) (:tx-counter n) (:id e) (:tx-counter e)))
;;
;;
;;(defn prepare-ack
;;  [^AckEvent e]
;;  [(.cmd_type e) (.-id e) (.-restart_counter e) (.-tx_counter e) (.-receiver_id e) (.-receiver_tx_counter e)])
;;
;;
;;(defn restore-ack
;;  [^PersistentVector v]
;;  (if (and
;;        (vector? v)
;;        (= 6 (count v))
;;        (every? true? (map #(%1 %2) [#(= % (:ack event-code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
;;    (apply ->AckEvent v)
;;    (throw (ex-info "AckEvent vector has invalid structure" {:ack-vec v}))))
;;
;;
;;(defn new-joining
;;  [^Node n]
;;  (->JoiningEvent
;;    (:joining event-code) (:id n) (:restart-counter n) (:tx-counter n) (.-host n) (.-port n)))
;;
;;
;;(defn prepare-joining
;;  [^PingEvent this]
;;  [(.cmd_type this) (.-id this) (.-restart_counter this) (.-tx_counter this) (.receiver_id this)])
;;
;;
;;(defn restore-joining
;;  [^PersistentVector ping-vec]
;;  (if (and
;;        (vector? ping-vec)
;;        (= 5 (count ping-vec))
;;        (every? true? (map #(%1 %2) [#(= % (:ping event-code)) uuid? nat-int? nat-int? uuid?] ping-vec)))
;;    (apply ->PingEvent ping-vec)
;;    (throw (ex-info "PingEvent vector has invalid structure" {:ping-vec ping-vec}))))
;;
;;
;;(comment
;;
;;  (def cluster (new-cluster {:id          #uuid "f876678d-f544-4fb8-a848-dc2c863aba6b"
;;                             :name        "cluster1"
;;                             :description "Test cluster1"
;;                             :secret-key  "0123456789abcdef0123456789abcdef"
;;                             :root-nodes  [{:host "127.0.0.1" :port 5376} {:host "127.0.0.1" :port 5377}]
;;                             :nspace      "test-ns1"
;;                             :tags        ["dc1" "rssys"]}))
;;
;;  (def node1 (new-node {:name "node1" :host "127.0.0.1" :port 5376 :cluster cluster :tags ["dc1" "node1"]}))
;;  (def node2 (new-node {:name "node2" :host "127.0.0.2" :port 5377 :cluster cluster :tags ["dc1" "node2"]}))
;;
;;
;;  (start node1 #(println (String. %)))
;;
;;  (udp/send-packet (.getBytes "hello world") "127.0.0.1" 5376)
;;
;;  (stop node1)
;;
;;  (def ping1 (new-ping (node-value node1) (random-uuid)))
;;
;;  (def prepared-ping1 (prepare ping1))
;;  (restore-ping prepared-ping1)
;;  (restore (empty-ping) prepared-ping1)
;;
;;  (count (serialize prepared-ping1))
;;  (count (serialize ping1))
;;
;;  (def ack1 (new-ack (node-value node2) ping1))
;;  )
;;
;;
