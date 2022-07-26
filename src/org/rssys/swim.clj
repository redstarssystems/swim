(ns org.rssys.swim
  "SWIM functions, specs and domain entities"
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as string]
    [clojure.test.check.generators :as gen]
    [org.rssys.scheduler :as scheduler]
    [org.rssys.udp :as udp]
    [soothe.core :as sth])
  (:import
    (clojure.lang
      Atom)
    (java.io
      Writer)
    (java.time
      Instant)
    (java.time.temporal
      ChronoUnit)))


;;;;;;;;;;;;;;;;
;; SWIM spec
;;;;;;;;;;;;;;;;


(def ne-string-gen #(gen/fmap (fn [x] (str "not-empty-string-" x)) (gen/choose 1 1000000)))
(s/def ::ne-string (s/with-gen (s/and string? (complement string/blank?)) ne-string-gen))
(sth/def ::ne-string "Should be not a blank string.")


(def timestamp-gen #(gen/fmap (fn [x] (.plus (Instant/now) x ChronoUnit/SECONDS)) (gen/choose 0 10000)))
(s/def ::timestamp (s/with-gen #(instance? Instant %) timestamp-gen))
(sth/def ::timestamp "Should be an Instant, UTC timestamp.")


(def host-gen #(gen/fmap (fn [x] (if (even? x) "localhost" "127.0.0.1")) (gen/choose 1 10)))
(s/def ::host (s/with-gen ::ne-string host-gen))            ;; hostname or ip address
(sth/def ::host "Should be valid hostname or IP address.")


(s/def ::port (s/and pos-int? #(< 1023 % 65536)))
(sth/def ::port "Should be a valid port number")


(s/def ::id uuid?)
(sth/def ::id "Should be a UUID, a unique identifier")


(def name-gen #(gen/fmap (fn [x] (str "name-" x)) (gen/choose 1 1000)))
(s/def ::name (s/with-gen ::ne-string name-gen))
(sth/def ::name "Should be a name, non empty string")


(def description-gen #(gen/fmap (fn [x] (str "Description string - " x)) (gen/choose 1 1000)))
(s/def ::description (s/with-gen string? description-gen))


(def secret-key-gen (fn [] (gen/such-that #(not= % "") gen/string-alphanumeric)))
(s/def ::secret-key (s/with-gen ::ne-string secret-key-gen))
(sth/def ::secret-key "Should be a secret key, non empty string")


(s/def ::start-time ::timestamp)


(s/def ::server-state #{:running :stopped})
(sth/def ::server-state (format "Should be one of %s" (s/describe ::server-state)))


;; Cluster specs

(s/def ::root-node
  (s/keys :req-un [::host
                   ::port]))


(s/def ::root-nodes (s/coll-of ::root-node :gen-max 3))
(gen/sample (s/gen ::root-nodes))


(def tag-gen #(gen/fmap (fn [x] (str "tag-" x)) (gen/choose 1 1000)))
(s/def ::tag (s/with-gen ::ne-string tag-gen))


(s/def ::tags (s/coll-of ::tag :gen-max 3))


(def nspace-gen #(gen/fmap (fn [x] (str "namespace" x)) (gen/choose 1 1000)))
(s/def ::nspace (s/with-gen ::ne-string nspace-gen))


(s/def ::cluster
  (s/keys :req-un [::id
                   ::name
                   ::description
                   ::secret-key
                   ::root-nodes
                   ::nspace
                   ::tags]))


;; node specs

;; NB: :stopped - about node, joining -> :normal -> :leave -> :dead - about cluster
(s/def ::status #{:stopped :joining :normal :suspicious :leave :dead})
(sth/def ::status (format "Should be one of %s" (s/describe ::status)))


(s/def ::access #{:direct :indirect})
(sth/def ::access (format "Should be one of %s" (s/describe ::access)))


(s/def ::neighbour-descriptor
  (s/keys :req-un [::id
                   ::status
                   ::access]))


(s/def ::neighbours-table (s/map-of ::id ::neighbour-descriptor :gen-max 3))


;; incremented after each node restart
(s/def ::restart-counter nat-int?)


;; incremented after each event occurred
(s/def ::tx-counter nat-int?)


(s/def ::max-packet-size nat-int?)


(s/def ::continue? boolean?)


(s/def ::server-packet-count nat-int?)


(s/def ::*udp-server
  (s/nilable
    (s/keys :req-un [::host
                     ::port
                     ::start-time
                     ::max-packet-size
                     ::server-state
                     ::continue?
                     ::server-packet-count])))


;;

(def object-gen #(gen/fmap (fn [_] (Object.)) (gen/return 1)))
(s/def ::object (s/with-gen any? object-gen))


(s/def ::scheduler-pool ::object)


(s/def ::ping-ids (s/coll-of ::id :gen-max 3))


(s/def ::ping-data (s/map-of ::id ::object :gen-max 3))


(s/def ::suspicious-node-ids (s/coll-of ::id :gen-max 3))


(s/def ::node
  (s/keys :req-un [::id
                   ::name
                   ::host
                   ::port
                   ::cluster
                   ::continue?
                   ::status
                   ::neighbours-table
                   ::*udp-server
                   ::restart-counter
                   ::scheduler-pool
                   ::tx-counter
                   ::ping-ids
                   ::ping-data
                   ::tags]))



(s/def ::*node
  (s/and
    #(instance? Atom %)
    #(s/valid? ::node (deref %))))


;;;;;;;;;;;;;;;;;;;
;; Domain entities
;;;;;;;;;;;;;;;;;;;

(declare cluster-str)


(defrecord Cluster [id name description secret-key root-nodes nspace tags]
           Object
           (toString [this] (cluster-str this)))


(defmethod print-method Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


(defmethod print-dup Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


(defn cluster-str
  "Returns String representation of Cluster"
  ^String
  [^Cluster cluster]
  (str (into {} (assoc cluster :secret-key "***censored***"))))


(defn new-cluster
  "Returns new Cluster instance."
  [{:keys [id name description secret-key root-nodes nspace tags]}]
  (let [cluster (->Cluster (or id (random-uuid)) name description secret-key root-nodes nspace tags)]
    (if-not (s/valid? ::cluster cluster)
      (throw (ex-info "Cluster values should correspond to spec" (sth/explain-data ::cluster cluster)))
      cluster)))


(defprotocol INode
  "Node protocol"
  :extend-via-metadata true
  (start [node process-cb-fn] "Start this node and process each message using given callback function in a new Virtual Thread")
  (stop [node] "Stop this node")
  (value [node] "Get node state value")
  (join [node] "Join this node to the cluster")
  (leave [node] "Leave the cluster"))


(declare node-start)
(declare node-stop)
(declare node-value)
(declare node-to-string)
(declare node-join)
(declare node-leave)


(defrecord Node [id
                 name
                 host
                 port
                 cluster
                 continue?
                 status
                 neighbours-table
                 *udp-server
                 restart-counter
                 scheduler-pool
                 tx-counter
                 ping-ids
                 ping-data
                 tags]

           Object
           (toString [this] (node-to-string this)))


(defrecord NodeObject [*node]

           INode
           (start [this process-cb-fn] (node-start this process-cb-fn))
           (stop [this] (node-stop this))
           (value [this] (node-value this))
           (join [this] (node-join this))
           (leave [this] (node-leave this)))


(defn new-node
  "Returns NodeObject with new Node inside."
  [{:keys [id name host port cluster tags]}]
  (let [node (map->Node {:id               (or id (random-uuid))
                         :name             name
                         :host             host
                         :port             port
                         :cluster          cluster
                         :continue?        true
                         :status           :stopped
                         :neighbours-table {}
                         :*udp-server      nil
                         :restart-counter  0
                         :scheduler-pool   (scheduler/mk-pool)
                         :tx-counter       0
                         :ping-ids         []
                         :ping-data        {}
                         :tags             tags})]
    (if-not (s/valid? ::node node)
      (throw (ex-info "Node values should correspond to spec" (sth/explain-data ::node node)))
      (->NodeObject (atom node)))))



(defn node-value
  "Returns node value"
  [node-object]
  (-> node-object :*node deref))


(defn node-join
  "Join this node to the cluster"
  [node-object]
  ;; TODO: implement join to cluster
  (swap! (:*node node-object) assoc :status :normal))


(defn node-start
  "Start the node"
  [node-object process-cb-fn]
  (let [{:keys [host port]}  (node-value node-object)
        *udp-server (udp/start host port process-cb-fn)]
    (when-not (s/valid? ::*udp-server @*udp-server)
      (throw (ex-info "UDP server values should correspond to spec" (sth/explain-data ::*udp-server @*udp-server))))
    (swap! (:*node node-object) assoc :*udp-server *udp-server :status :leave)))


(defn node-leave
  "Leave the cluster"
  [node-object]
  ;; TODO: implement leave cluster
  (swap! (:*node node-object) assoc :status :leave))


(defn node-stop
  "Stop the node and leave the cluster.
  Stop UDP server.
  Forcefully interrupt all running tasks in scheduler and does not wait.
  Scheduler pool is reset to a fresh new pool preserving the original size."
  [node-object]
  (let [{:keys [*udp-server scheduler-pool]} (node-value node-object)]
    (node-leave node-object)
    (scheduler/stop-and-reset-pool! scheduler-pool :strategy :kill)
    (swap! (:*node node-object) assoc
      :*udp-server (udp/stop *udp-server)
      :status :stopped)))


(defn calc-n
  "Calculate how many nodes should we notify.
  n - number of nodes in the cluster."
  [^long n]
  (int (Math/floor (/ (Math/log n) (Math/log 2)))))


(comment

  (def cluster (new-cluster {:id          #uuid "f876678d-f544-4fb8-a848-dc2c863aba6b"
                             :name        "cluster1"
                             :description "Test cluster1"
                             :secret-key  "0123456789abcdef0123456789abcdef"
                             :root-nodes  [{:host "127.0.0.1" :port 5376} {:host "127.0.0.1" :port 5377}]
                             :nspace      "test-ns1"
                             :tags        ["dc1" "rssys"]}))

  (def no1 (new-node {:name "node1" :host "127.0.0.1" :port 5376 :cluster cluster :tags ["dc1" "node1"]}))
  (prn no1 )
  (type no1)

  (start no1 #(println (String. %)))

  (udp/send-packet (.getBytes "hello world") "127.0.0.1" 5376)

  (stop no1)
  )
