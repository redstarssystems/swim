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


