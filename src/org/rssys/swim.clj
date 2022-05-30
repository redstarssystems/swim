(ns org.rssys.swim
  "SWIM functions, specs and domain entities"
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as string]
    [org.rssys.scheduler :as scheduler]
    [org.rssys.udp :as udp]
    [soothe.core :as sth])
  (:import
    (clojure.lang
      Atom)
    (java.io
      Writer)
    (java.time
      LocalDateTime)))


;;;;;;;;;;;;;;;;
;; SWIM spec
;;;;;;;;;;;;;;;;

;; not empty string
(s/def ::ne-string (s/and string? (complement string/blank?)))
(sth/def ::ne-string "Should be not a blank string.")

(s/def ::timestamp #(instance? LocalDateTime %))
(sth/def ::timestamp "Should be LocalDateTime instance.")


;; network specs
(s/def ::host ::ne-string)

(s/def ::port (s/and pos-int? #(< 1023 % 65536)))
(sth/def ::port "Should be a valid port number")


;; common specs
(s/def ::id uuid?)
(s/def ::name ::ne-string)
(s/def ::description string?)
(s/def ::secret-key ::ne-string)
(s/def ::start-time ::timestamp)

(s/def ::server-state #{:running :stopped})
(sth/def ::server-state (format "Should be one of %s" (s/describe ::server-state)))


;; cluster specs
(s/def ::root-node
  (s/keys :req-un [::host
                   ::port]))


(s/def ::root-nodes (s/coll-of ::root-node))
(s/def ::tag ::ne-string)
(s/def ::tags (s/coll-of ::tag))
(s/def ::nspace ::ne-string)


(s/def ::cluster
  (s/keys :req-un [::id
                   ::name
                   ::description
                   ::secret-key
                   ::root-nodes
                   ::nspace
                   ::tags]))


;; node specs
(s/def ::status #{:joining :normal :dead :suspicious :leave :stopped})
(sth/def ::status (format "Should be one of %s" (s/describe ::status)))

(s/def ::access #{:direct :indirect})
(sth/def ::access (format "Should be one of %s" (s/describe ::access)))


(s/def ::neighbour-descriptor
  (s/keys :req-un [::id
                   ::status
                   ::access]))


(s/def ::neighbours-table (s/map-of ::id ::neighbour-descriptor))

(s/def ::restart-counter nat-int?)
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


(s/def ::object any?)
(s/def ::scheduler-pool ::object)
(s/def ::ping-ids (s/coll-of ::id))
(s/def ::ping-data (s/map-of ::id ::object))
(s/def ::suspicious-node-ids (s/coll-of ::id))


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
  (state [node] "Get node state value"))


(declare node-start)
(declare node-stop)
(declare node-state)
(declare node-to-string)


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


           INode
           (start [this process-cb-fn] (node-start this process-cb-fn))
           (stop [this] (node-stop this))
           (state [this] (node-state this))

           Object
           (toString [this] (node-to-string this)))


(defn new-node
  "Returns Atom with new Node inside."
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
      (atom node))))



(defn join-to-cluster
  [*node]
  ;; TODO: implement join to cluster
  (swap! *node assoc :status :normal))



(defn node-start
  "Start node and join to the cluster"
  [*node process-cb-fn]
  (let [{:keys [host port]} @*node
        *udp-server (udp/server-start host port process-cb-fn)]
    (when-not (s/valid? ::*udp-server @*udp-server)
      (throw (ex-info "UDP server values should correspond to spec" (sth/explain-data ::*udp-server @*udp-server))))
    (swap! *node assoc :*udp-server *udp-server :status :joining)
    (join-to-cluster *node)))



(defn leave-cluster
  [*node]
  ;; TODO: implement leave cluster
  (swap! *node assoc :status :leave))


(defn node-stop
  "Stop node and leave the cluster"
  [*node]
  (let [{:keys [*udp-server]} @*node]
    (leave-cluster *node)
    (udp/server-stop *udp-server)
    (swap! *node assoc :*udp-server nil :status :stopped)))


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

  (def *n1 (new-node {:name "node1" :host "127.0.0.1" :port 5376 :cluster cluster :tags ["dc1" "node1"]}))
  (prn @n1)
  )
