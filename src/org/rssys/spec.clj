(ns org.rssys.spec
  "SWIM spec"
  (:require
    [clojure.spec.alpha :as s]))


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
(s/def ::ping-events (s/map-of ::neighbour-id ::ping-event)) ;; buffer for whom we sent a ping and waiting for an ack

(s/def ::neighbour-tx ::tx)
(s/def ::ack-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::neighbour-id ::neighbour-tx]))
(s/def ::probe-ack-event (s/keys :req-un [::cmd-type ::id ::host ::port ::status ::restart-counter ::tx ::neighbour-id ::neighbour-tx]))


;; ::neighbour-id - dead node
(s/def ::dead-event (s/keys :req-un [::cmd-type ::id ::restart-counter ::tx ::neighbour-id ::neighbour-tx]))

(s/def ::probe-event (s/keys :req-un [::cmd-type ::id ::host ::port ::restart-counter ::tx ::neighbour-host ::neighbour-port]))
(s/def ::anti-entropy-event (s/keys :req-un [::cmd-type ::anti-entropy-data]))

(s/def ::scheduler-pool ::object)
(s/def ::*udp-server ::object)


;; buffer for outgoing events which we propagate with ping and ack events
(s/def ::outgoing-event-queue vector?)

(s/def ::ping-round-buffer (s/coll-of ::neighbour-id))


(s/def ::node
  (s/keys :req-un [::id ::host ::port ::cluster ::status ::neighbours ::restart-counter
                   ::tx ::ping-events ::payload ::scheduler-pool ::*udp-server ::outgoing-event-queue
                   ::ping-round-buffer]))



(defn problems
  [explain-data]
  {:problems (vec (::s/problems explain-data))})

