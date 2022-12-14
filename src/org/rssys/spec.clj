(ns org.rssys.spec
  "SWIM spec"
  (:require
    [clojure.spec.alpha :as s]))


;;;;;;;;;;;;;
;;; Basic specs

(s/def ::id uuid?)
(s/def ::neighbour-id ::id)
(s/def ::host string?)
(s/def ::port (s/and pos-int? #(< % 65536)))
(s/def ::intermediate-id ::id)
(s/def ::intermediate-host ::host)
(s/def ::intermediate-port ::port)
(s/def ::neighbour-host ::host)
(s/def ::neighbour-port ::port)
(s/def ::name string?)
(s/def ::desc string?)
(def status-set #{:stop :join :alive :suspect :left :dead :unknown})
(s/def ::status status-set)
(s/def ::access #{:direct :indirect})                       ;; 0 - :direct, 1 - :indirect
(s/def ::object any?)
(s/def ::tags set?)                                         ;; #{"dc1" "test"}
(s/def ::nspace (s/or :symbol symbol? :keyword keyword? :string string?)) ;; cluster namespace
(s/def ::secret-token string?)                              ;; string token for secret key gen to access to cluster
(s/def ::secret-key ::object)                               ;; 256-bit SecretKey generated from secret token
(s/def ::cluster-size nat-int?)                             ;; number of nodes in the cluster
(s/def ::cluster (s/keys :req-un [::id ::name ::desc ::secret-token ::nspace ::tags] :opt-un [::secret-key ::cluster-size]))
(s/def ::probe-key any?)                                    ;; unique key for probe <--> probe-ack

(s/def ::restart-counter nat-int?)                          ;; increase every node restart. part of incarnation.
(s/def ::tx nat-int?)                                       ;; increase every event on node. part of incarnation.
(s/def ::payload ::object)                                  ;; some data attached to node and propagated to cluster
(s/def ::updated-at nat-int?)

(s/def ::neighbour-status ::status)
(s/def ::neighbour-restart-counter ::restart-counter)
(s/def ::ts nat-int?)                                       ;; timestamp as long

(s/def ::anti-entropy-data (s/coll-of vector?))

(s/def ::attempt-number pos-int?)

(s/def ::neighbour-tx ::tx)

(s/def ::old-cluster-size ::cluster-size)
(s/def ::new-cluster-size ::cluster-size)


;;;;;;;;;;
;; Event specs

(s/def ::probe-event
  (s/keys :req-un [::cmd-type ::id ::host ::port ::restart-counter ::tx ::neighbour-host
                   ::neighbour-port ::probe-key]))


(s/def ::probe-ack-event
  (s/keys :req-un [::cmd-type ::id ::host ::port ::status ::restart-counter ::tx
                   ::neighbour-id ::probe-key]))


(s/def ::ping-event
  (s/keys :req-un [::cmd-type ::id ::host ::port ::restart-counter ::tx
                   ::neighbour-id ::attempt-number]))


(s/def ::ack-event
  (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx
                   ::neighbour-id ::neighbour-tx ::attempt-number]))


(s/def ::indirect-ping-event
  (s/keys :req-un [::cmd-type ::id ::host ::port ::restart-counter ::tx
                   ::intermediate-id ::intermediate-host ::intermediate-port
                   ::neighbour-id ::neighbour-host ::neighbour-port ::attempt-number]))


(s/def ::indirect-ack-event
  (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::status ::host ::port
                   ::intermediate-id ::intermediate-host ::intermediate-port
                   ::neighbour-id ::neighbour-host ::neighbour-port ::attempt-number]))


;; alive node is a neighbour
(s/def ::alive-event
  (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx
                   ::neighbour-id ::neighbour-restart-counter ::neighbour-tx]))


(s/def ::new-cluster-size-event
  (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx
                   ::old-cluster-size ::new-cluster-size]))


;; ::neighbour-id - dead node
(s/def ::dead-event
  (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx
                   ::neighbour-id ::neighbour-restart-counter ::neighbour-tx]))


(s/def ::anti-entropy-event
  (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx
                   ::anti-entropy-data]))


(s/def ::join-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::host ::port]))


;; suspect node is a neighbour
(s/def ::suspect-event
  (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx
                   ::neighbour-id ::neighbour-restart-counter ::neighbour-tx]))


(s/def ::left-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx]))

(s/def ::payload-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::payload]))


;;;;;;;;;;
;; Domain specs

(s/def ::node
  (s/keys :req-un [::id ::host ::port ::cluster ::status ::neighbours ::restart-counter
                   ::tx ::ping-events ::indirect-ping-events
                   ::payload ::scheduler-pool ::*udp-server ::outgoing-events
                   ::ping-round-buffer ::probe-events]))


(s/def ::neighbour-node
  (s/keys :req-un [::id ::host ::port ::status ::access ::restart-counter
                   ::tx ::payload ::updated-at]))


(s/def ::neighbours (s/map-of ::neighbour-id ::neighbour-node))


;;;;;;;;;;;;
;; buffers

(s/def ::probe-events (s/map-of ::probe-key (s/nilable ::probe-ack-event)))


;; buffer for ping events we sent and waiting for an ack
(s/def ::ping-events (s/map-of ::neighbour-id ::ping-event))


;; buffer for indirect pings we sent and waiting for an ack
(s/def ::indirect-ping-events (s/map-of ::neighbour-id ::indirect-ping-event))


;; buffer for outgoing events which we propagate via piggieback with ping events
(s/def ::outgoing-events vector?)


;; collection of neighbour ids for future ping
(s/def ::ping-round-buffer (s/coll-of ::neighbour-id))


(s/def ::scheduler-pool ::object)
(s/def ::*udp-server ::object)


;;;;;;;;;;

(defn problems
  [explain-data]
  {:problems (vec (::s/problems explain-data))})

