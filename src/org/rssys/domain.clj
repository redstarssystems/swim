(ns org.rssys.domain
  (:import
    (java.io
      Writer)))


(defn cluster-str
  "Returns String representation of Cluster"
  ^String
  [cluster]
  (str (into {} (assoc cluster :secret-token "***censored***"))))


(defrecord Cluster [id name desc secret-token nspace tags secret-key cluster-size]
           Object
           (toString [this] (cluster-str this)))


(defmethod print-method Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


(defmethod print-dup Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


;;;;;


(defrecord NeighbourNode [id
                          host
                          port
                          status
                          access
                          restart-counter
                          tx
                          payload
                          updated-at])


(defrecord Node [id
                 host
                 port
                 cluster
                 status
                 neighbours
                 restart-counter
                 tx
                 ping-events
                 payload
                 scheduler-pool
                 *udp-server
                 outgoing-event-queue
                 ping-round-buffer]
           Object
           (toString [this] (.toString this)))


(defrecord NodeObject [*node])


