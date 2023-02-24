(ns org.rssys.swim.domain
  (:import
    (java.io
      Writer)))



;;;;;


(defrecord NeighbourNode [id
                          host
                          port
                          status
                          access
                          restart-counter
                          events-tx
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
                 indirect-ping-events
                 payload
                 *udp-server
                 outgoing-events
                 ping-round-buffer
                 probe-events]
           Object
           (toString [this] (.toString this)))


(defrecord NodeObject [*node])


