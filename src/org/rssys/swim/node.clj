(ns org.rssys.swim.node
  (:require [clojure.spec.alpha :as s]
            [org.rssys.swim.spec :as spec])
  (:import (clojure.lang Keyword)))

(defrecord Node [config
                 cluster
                 id
                 host
                 port
                 restart-counter
                 status
                 neighbours
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


(def default-config
  {:enable-diag-tap?                    true                ;; Put diagnostic data to tap>
   :max-udp-size                        1432                ;; Max size of UDP packet in bytes
   :ignore-max-udp-size?                false               ;; by default, we prevent sending UDP more than :max-udp-size
   :max-payload-size                    256                 ;; Max payload size in bytes
   :max-anti-entropy-items              2                   ;; Max items number in anti-entropy
   :max-ping-without-ack-before-suspect 2                   ;; How many pings without ack before node became suspect
   :max-ping-without-ack-before-dead    4                   ;; How many pings without ack before node considered as dead


   :ping-heartbeat-ms                   1000                ;; Send ping+events to neighbours every N ms
   :ack-timeout-ms                      200                 ;; How much time we wait for an ack event before next ping will be sent
   :max-join-time-ms                    500                 ;; How much time node awaits join confirmation before timeout
   :rejoin-if-dead?                     true                ;; After node join, if cluster consider this node as dead, then do rejoin
   :rejoin-max-attempts                 10                  ;; How many times try to rejoin
   })


(defn new-node-object
  "Create new NodeObject instance.

  Required params:
  `cluster` - Cluster instance.
   `host`  - hostname or ip address.
   `port`  - UDP port.

   Optional params:
   `config` - map with Node parameters or `default-config` is used.
   `id` - uuid value or random uuid is used.
   `restart-counter` - positive number or current milliseconds is used.
   `neighbours` - map of neighbours or empty map is used.

   Returns new NodeObject instance as Node in atom ."
  [{:keys [config cluster id host port restart-counter neighbours]}]
  (let [node (map->Node
               {:config               (or config default-config)
                :cluster              cluster
                :id                   (or id (random-uuid))
                :host                 host
                :port                 port
                :restart-counter      (or restart-counter (System/currentTimeMillis))
                :status               :stop
                :neighbours           (or neighbours {})
                :tx                   0
                :ping-events          {}               ;; active direct pings
                :indirect-ping-events {}               ;; active indirect pings
                :payload              {}               ;; data that this node claims in cluster about itself
                :*udp-server          nil
                :outgoing-events      []               ;; outgoing events that we'll send to random logN neighbours next time
                :ping-round-buffer    []               ;; we take logN neighbour ids to send events from event queue
                :probe-events         {}               ;; outgoing probe events
                })]
    (if-not (s/valid? ::spec/node node)
      (throw (ex-info "Invalid node data" (->> node (s/explain-data ::spec/node) spec/problems)))
      (map->NodeObject {:*node (atom node)}))))


(defn get-value
  "Get node value"
  ^Node
  [^NodeObject this]
  @(:*node this))


(defn get-config
  "Get config"
  [^NodeObject this]
  (.-config (get-value this)))


(defn get-cluster
  "Get cluster "
  [^NodeObject this]
  (.-cluster (get-value this)))


(defn get-id
  "Get node id"
  [^NodeObject this]
  (.-id (get-value this)))

(defn get-host
  "Get node host"
  ^String
  [^NodeObject this]
  (.-host (get-value this)))


(defn get-port
  "Get node port"
  ^long
  [^NodeObject this]
  (.-port (get-value this)))


(defn get-restart-counter
  "Get node restart counter"
  ^long
  [^NodeObject this]
  (.-restart_counter (get-value this)))


(defn get-status
  "Get current node status"
  ^Keyword
  [^NodeObject this]
  (.-status (get-value this)))


(defn get-neighbours
  "Get all node neighbours. Returns map of {:id :neighbour-node}."
  [^NodeObject this]
  (.-neighbours (get-value this)))

(defn get-tx
  "Get node tx"
  ^long [^NodeObject this]
  (.-tx (get-value this)))


(defn get-ping-events
  "Get map of active ping events which we sent to neighbours"
  [^NodeObject this]
  (.-ping_events (get-value this)))


(defn get-indirect-ping-events
  "Get map of active indirect ping events which we received from neighbours"
  [^NodeObject this]
  (.-indirect_ping_events (get-value this)))


(defn get-payload
  "Get node payload"
  [^NodeObject this]
  (.-payload (get-value this)))


(defn get-udp-server
  "Get UDP server"
  [^NodeObject this]
  (.-_STAR_udp_server (get-value this)))


(defn get-outgoing-events
  "Get vector of prepared outgoing events"
  [^NodeObject this]
  (.-outgoing_events (get-value this)))


(defn get-ping-round-buffer
  "Returns vector of ids intended for pings."
  [^NodeObject this]
  (.-ping_round_buffer (get-value this)))


(defn get-probe-events
  "Get map of probe events which we received from neighbours"
  [^NodeObject this]
  (.-probe_events (get-value this)))
