(ns org.rssys.swim.spec
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
(s/def ::namespace (s/or :symbol symbol? :keyword keyword? :string string?)) ;; cluster namespace
(s/def ::password (s/and string? #(>= (.length %) 16)))     ;; password for secret key generation to access to cluster
(s/def ::cluster-size pos-int?)                             ;; number of nodes in the cluster
(s/def ::cluster (s/keys :req-un [::id ::name ::password ::namespace] :opt-un [::desc ::cluster-size ::tags]))
(s/def ::probe-key any?)                                    ;; unique key for probe <--> probe-ack

(s/def ::restart-counter nat-int?)                          ;; increase every node restart. part of incarnation.
(s/def ::tx nat-int?)                                       ;; increase every event on node. part of incarnation.
(s/def ::payload ::object)                                  ;; some data attached to node and propagated to cluster
(s/def ::updated-at nat-int?)
(s/def ::event-code nat-int?)

(s/def ::neighbour-status ::status)
(s/def ::neighbour-restart-counter ::restart-counter)
(s/def ::ts nat-int?)                                       ;; timestamp as long

(s/def ::anti-entropy-data (s/coll-of vector?))

(s/def ::attempt-number pos-int?)

(s/def ::neighbour-tx ::tx)

(s/def ::events-tx (s/map-of ::event-code ::tx))

(s/def ::old-cluster-size ::cluster-size)
(s/def ::new-cluster-size ::cluster-size)


;;;;;;;;;;
;; Event specs

(s/def ::probe-event
  (s/keys :req-un [::event-code ::id ::host ::port ::restart-counter ::tx ::neighbour-host
                   ::neighbour-port ::probe-key]))


(s/def ::probe-ack-event
  (s/keys :req-un [::event-code ::id ::host ::port ::status ::restart-counter ::tx
                   ::neighbour-id ::probe-key]))


(s/def ::ping-event
  (s/keys :req-un [::event-code ::id ::host ::port ::restart-counter ::tx
                   ::neighbour-id ::attempt-number ::ts]))


(s/def ::ack-event
  (s/keys :req-un [::event-code ::id ::restart-counter ::tx
                   ::neighbour-id ::neighbour-tx ::attempt-number ::ts]))


(s/def ::indirect-ping-event
  (s/keys :req-un [::event-code ::id ::host ::port ::restart-counter ::tx
                   ::intermediate-id ::intermediate-host ::intermediate-port
                   ::neighbour-id ::neighbour-host ::neighbour-port ::attempt-number ::ts]))


(s/def ::indirect-ack-event
  (s/keys :req-un [::event-code ::id ::restart-counter ::tx ::status ::host ::port
                   ::intermediate-id ::intermediate-host ::intermediate-port
                   ::neighbour-id ::neighbour-host ::neighbour-port ::attempt-number ::ts]))


(s/def ::alive-event
  (s/keys :req-un [::event-code ::id ::restart-counter ::tx
                   ::neighbour-id ::neighbour-restart-counter ::neighbour-tx
                   ::neighbour-host ::neighbour-port]))


(s/def ::new-cluster-size-event
  (s/keys :req-un [::event-code ::id ::restart-counter ::tx
                   ::old-cluster-size ::new-cluster-size]))


(s/def ::dead-event
  (s/keys :req-un [::event-code ::id ::restart-counter ::tx
                   ::neighbour-id ::neighbour-restart-counter ::neighbour-tx]))


(s/def ::anti-entropy-event
  (s/keys :req-un [::event-code ::id ::restart-counter ::tx
                   ::anti-entropy-data]))


(s/def ::join-event (s/keys :req-un [::event-code ::id ::restart-counter ::tx ::host ::port]))

(s/def ::left-event (s/keys :req-un [::event-code ::id ::restart-counter ::tx]))

(s/def ::payload-event (s/keys :req-un [::event-code ::id ::restart-counter ::tx ::payload]))

;;;;;;;;;;
;; Node config spec

(s/def :config/enable-diag-tap? boolean?)
(s/def :config/max-udp-size pos-int?)
(s/def :config/ignore-max-udp-size?  boolean?)
(s/def :config/max-payload-size pos-int?)
(s/def :config/max-anti-entropy-items nat-int?)
(s/def :config/max-ping-without-ack-before-suspect pos-int?)
(s/def :config/max-ping-without-ack-before-dead pos-int?)
(s/def :config/ping-heartbeat-ms pos-int?)
(s/def :config/ack-timeout-ms pos-int?)
(s/def :config/max-join-time-ms pos-int?)
(s/def :config/rejoin-if-dead? boolean?)
(s/def :config/rejoin-max-attempts pos-int?)


(s/def ::config (s/keys :req-un [:config/enable-diag-tap?
                                 :config/max-udp-size
                                 :config/ignore-max-udp-size?
                                 :config/max-payload-size
                                 :config/max-anti-entropy-items
                                 :config/max-ping-without-ack-before-suspect
                                 :config/max-ping-without-ack-before-dead
                                 :config/ping-heartbeat-ms
                                 :config/ack-timeout-ms
                                 :config/max-join-time-ms
                                 :config/rejoin-if-dead?
                                 :config/rejoin-max-attempts]))



;;;;;;;;;;
;; Domain specs

(s/def ::node
  (s/keys :req-un [::config ::id ::host ::port ::cluster ::status ::neighbours ::restart-counter
                   ::tx ::ping-events ::indirect-ping-events
                   ::payload ::*udp-server ::outgoing-events
                   ::ping-round-buffer ::probe-events]))


(s/def ::neighbour-node
  (s/keys :req-un [::id ::host ::port ::status ::access ::restart-counter
                   ::events-tx ::payload ::updated-at]))


(s/def ::neighbours (s/map-of ::neighbour-id ::neighbour-node))


;;;;;;;;;;;;
;; buffers

(s/def ::probe-events (s/map-of ::probe-key (s/nilable ::probe-ack-event)))

(s/def ::ping-key (s/tuple ::neighbour-id ::ts))

;; buffer for ping events we sent and waiting for an ack
(s/def ::ping-events (s/map-of ::ping-key ::ping-event))


;; buffer for indirect pings we sent and waiting for an ack
(s/def ::indirect-ping-events (s/map-of ::ping-key ::indirect-ping-event))


;; buffer for outgoing events which we propagate via pigпieback with ping events
(s/def ::outgoing-events vector?)


;; collection of neighbour ids for future ping
(s/def ::ping-round-buffer (s/coll-of ::neighbour-id))


(s/def ::*udp-server ::object)

;;;;;;;;;;

(defn problems
  [explain-data]
  {:problems (vec (::s/problems explain-data))})

