(ns org.rssys.swim.node
  (:require [clojure.spec.alpha :as s]
            [org.rssys.swim.cluster]
            [org.rssys.swim.event]
            [org.rssys.swim.spec :as spec]
            [org.rssys.swim.util :refer [d> serialize]])
  (:import (clojure.lang Keyword)
           (java.util UUID)
           (org.rssys.swim.cluster Cluster)
           (org.rssys.swim.event ISwimEvent IndirectPingEvent PingEvent ProbeAckEvent ProbeEvent)))

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


(def alive-status-set #{:alive :suspect})

;;;;;;;;;;;;;;;;;;;;;
;; NodeObject Getters
;;;;;;;;;;;;;;;;;;;;;


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
  ^Cluster
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


(defn get-cluster-size
  "Get cluster size"
  ^long [^NodeObject this]
  (.-cluster_size (get-cluster this)))


(defn get-ping-event
  "Returns active ping event by key [uuid ts] if exists, otherwise nil"
  ^PingEvent
  [^NodeObject this ping-key]
  (get (get-ping-events this) ping-key))


(defn get-indirect-ping-event
  "Returns active indirect ping event by key [uuid ts] if exists, otherwise nil"
  ^IndirectPingEvent
  [^NodeObject this  ping-key]
  (get (get-indirect-ping-events this) ping-key))


(defn get-probe-event
  "Get active probe event if exist"
  ^PingEvent
  [^NodeObject this ^UUID probe-key]
  (get (get-probe-events this) probe-key))

(defn alive-node?
  "Returns true if node has alive statuses."
  [^NodeObject this]
  (boolean (alive-status-set (get-status this))))



;;;;;;;;;;;;;;;;;;;;;
;; NodeObject Setters
;;;;;;;;;;;;;;;;;;;;;


(defn set-config
  "Set config for this node"
  [^NodeObject this config]
  (cond
    (not (s/valid? ::spec/config config))
    (throw (ex-info "Invalid config data" (->> config (s/explain-data ::spec/config) spec/problems)))

    :else
    (do
      (d> this :set-config {:config config})
      (swap! (:*node this) assoc :config config))))


(defn set-cluster
  "Set cluster for this node"
  [^NodeObject this ^Cluster cluster]
  (cond
    (not (s/valid? ::spec/cluster cluster))
    (throw (ex-info "Invalid cluster data" (->> cluster (s/explain-data ::spec/cluster) spec/problems)))

    (not= :stop (get-status this))
    (throw (ex-info "Node is not stopped. Can't set new cluster value." {:current-status (get-status this)}))

    :else
    (do
      (d> this :set-cluster {:cluster (assoc cluster :secret-key ["***censored***"])})
      (swap! (:*node this) assoc :cluster cluster))))


(defn set-cluster-size
  "Set new cluster size"
  [^NodeObject this ^long new-cluster-size]
  (when-not (s/valid? ::spec/cluster-size new-cluster-size)
    (throw (ex-info "Invalid cluster size"
             (->> new-cluster-size (s/explain-data ::spec/cluster-size) spec/problems))))
  (d> this :set-cluster-size {:new-cluster-size new-cluster-size})
  (swap! (:*node this) assoc-in [:cluster :cluster-size] new-cluster-size))


(defn nodes-in-cluster
  "Returns number of nodes in cluster (neighbours + this). "
  [^NodeObject this]
  (-> this get-neighbours count inc))


(defn cluster-size-exceed?
  "Returns true if cluster size is reached its upper limit."
  [this]
  (let [node-number  (nodes-in-cluster this)
        cluster-size (get-cluster-size this)]
    (>= node-number cluster-size)))


(defn set-status
  "Set new status for this node"
  [^NodeObject this ^Keyword new-status]
  (when-not (s/valid? ::spec/status new-status)
    (throw (ex-info "Invalid node status" (->> new-status (s/explain-data ::spec/status) spec/problems))))
  (d> this :set-status {:old-status (get-status this) :new-status new-status})
  (swap! (:*node this) assoc :status new-status))


(defn set-dead-status
  "Set dead status for node."
  [^NodeObject this]
  (set-status this :dead))


(defn set-stop-status
  "Set stop status for node."
  [^NodeObject this]
  (set-status this :stop))


(defn set-left-status
  "Set left status for node."
  [^NodeObject this]
  (set-status this :left))


(defn set-join-status
  "Set join status for node."
  [^NodeObject this]
  (set-status this :join))


(defn set-alive-status
  "Set alive status for node."
  [^NodeObject this]
  (set-status this :alive))


(defn set-suspect-status
  "Set suspect status for node."
  [^NodeObject this]
  (set-status this :suspect))


(defn set-ping-round-buffer
  "Set vector of neighbour ids intended for pings."
  [^NodeObject this new-round-buffer]
  (when-not (s/valid? ::spec/ping-round-buffer new-round-buffer)
    (throw (ex-info "Invalid ping round buffer data" (->> new-round-buffer (s/explain-data ::spec/ping-round-buffer) spec/problems))))
  (d> this :set-ping-round-buffer {:ids-number (count new-round-buffer)})
  (swap! (:*node this) assoc :ping-round-buffer new-round-buffer))


(defn set-payload
  "Set new payload. Max size of payload is limited by `:config/max-payload-size`."
  [^NodeObject this payload]
  (let [actual-size      (alength ^bytes (serialize payload))
        max-payload-size (:max-payload-size (get-config this))]
    (when (> actual-size max-payload-size)
      (throw (ex-info "Payload size is too big" {:max-allowed max-payload-size :actual-size actual-size})))
    (d> this :set-payload {:payload payload :payload-size actual-size})
    (swap! (:*node this) assoc :payload payload)))


(defn set-restart-counter
  "Set node restart counter"
  [^NodeObject this ^long restart-counter]
  (when-not (s/valid? ::spec/restart-counter restart-counter)
    (throw (ex-info "Invalid restart counter data"
             (->> restart-counter (s/explain-data ::spec/restart-counter) spec/problems))))
  (d> this :set-restart-counter  {:restart-counter restart-counter})
  (swap! (:*node this) assoc :restart-counter restart-counter))


(defn set-tx
  "Set node tx"
  [^NodeObject this ^long tx]
  (when-not (s/valid? ::spec/tx tx)
    (throw (ex-info "Invalid tx data"
             (->> tx (s/explain-data ::spec/tx) spec/problems))))
  (d> this :set-tx {:tx tx})
  (swap! (:*node this) assoc :tx tx))


(defn inc-tx
  "Increment node tx"
  [^NodeObject this]
  (swap! (:*node this) assoc :tx (inc (get-tx this))))


(defn set-outgoing-events
  "Set outgoing events to new value"
  [^NodeObject this new-outgoing-events]
  (when-not (s/valid? ::spec/outgoing-events new-outgoing-events)
    (throw (ex-info "Invalid outgoing events data"
             (->> new-outgoing-events
               (s/explain-data ::spec/outgoing-events) spec/problems))))
  (d> :set-outgoing-events (get-id this) {:new-outgoing-events new-outgoing-events})
  (swap! (:*node this) assoc :outgoing-events new-outgoing-events))


(defn put-event
  "Put event to outgoing queue (FIFO)"
  [^NodeObject this ^ISwimEvent event]
  (when-not (instance? ISwimEvent event)
    (throw (ex-info "Event should be instance of ISwimEvent" {:event event})))
  (d> this :put-event {:event event})
  (swap! (:*node this) update-in [:outgoing-events] conj event))


(defn take-events
  "Take `n` outgoing events from FIFO buffer and return them.
   If `n` is omitted then take all events.
   Taken events will be removed from buffer."
  ([^NodeObject this ^long n]
   (let [events (->> this get-outgoing-events (take n) vec)]
     (d> this :take-events {:taken-events-count (count events)})
     (swap! (:*node this) update-in [:outgoing-events] subvec (count events))
     events))
  ([^NodeObject this]
   (take-events this (count (get-outgoing-events this)))))


(defn insert-ping
  "Insert new active ping event in a map.
  Returns pair [neighbour-id ts] as key of ping event in a map"
  [^NodeObject this ^PingEvent ping-event]
  (when-not (s/valid? ::spec/ping-event ping-event)
    (throw (ex-info "Invalid ping event data" (->> ping-event (s/explain-data ::spec/ping-event) spec/problems))))

  (let [ping-id [(.-neighbour_id ping-event) (.-ts ping-event)]]
    (d> this :insert-ping {:ping-event ping-event})
    (swap! (:*node this) assoc-in [:ping-events ping-id] ping-event)
    ping-id))


(defn delete-ping
  "Delete active ping event from map"
  [^NodeObject this ping-key]
  (when (get-ping-event this ping-key)
    (d> this :delete-ping {:neighbour-id (first ping-key) :ts (second ping-key)})
    (swap! (:*node this) update-in [:ping-events] dissoc ping-key)))


(defn insert-indirect-ping
  "Insert new active indirect ping event in map.
  Returns pair [neighbour-id ts] as key of indirect ping event in a map"
  [^NodeObject this ^IndirectPingEvent ping-event]
  (when-not (s/valid? ::spec/indirect-ping-event ping-event)
    (throw (ex-info "Invalid indirect ping event data"
             (->> ping-event (s/explain-data ::spec/indirect-ping-event) spec/problems))))
  (let [indirect-key [(.-neighbour_id ping-event) (.-ts ping-event)]]
    (d> this :insert-indirect-ping {:indirect-ping-event ping-event :indirect-id indirect-key})
    (swap! (:*node this) assoc-in [:indirect-ping-events indirect-key] ping-event)
    indirect-key))


(defn delete-indirect-ping
  "Delete active ping event from map"
  [^NodeObject this ping-key]
  (when (get-indirect-ping-event this ping-key)
    (d> this :delete-indirect-ping {:neighbour-id (first ping-key) :ts (second ping-key)})
    (swap! (:*node this) update-in [:indirect-ping-events] dissoc ping-key)))


(defn insert-probe
  "Insert probe key from probe event in a probe events map {probe-key nil}"
  [^NodeObject this ^ProbeEvent probe-event]
  (when-not (s/valid? ::spec/probe-event probe-event)
    (throw (ex-info "Invalid probe event data" (->> probe-event (s/explain-data ::spec/probe-event) spec/problems))))
  (d> this :upsert-probe {:probe-event probe-event})
  (swap! (:*node this) assoc-in [:probe-events (.-probe_key probe-event)] nil)
  (.-probe_key probe-event))


(defn delete-probe
  "Delete active probe event from map"
  [^NodeObject this ^UUID probe-key]
  (d> this :delete-probe {:neighbour-id probe-key})
  (swap! (:*node this) update-in [:probe-events] dissoc probe-key))


(defn upsert-probe-ack
  "Update existing or insert new probe ack event in map.
  `:probe-key` from event is used as a key in probe-events map."
  [^NodeObject this ^ProbeAckEvent probe-ack-event]
  (when-not (s/valid? ::spec/probe-ack-event probe-ack-event)
    (throw (ex-info "Invalid probe ack event data" (->> probe-ack-event (s/explain-data ::spec/probe-ack-event) spec/problems))))
  (d> this :upsert-probe-ack {:probe-ack-event probe-ack-event})
  (swap! (:*node this) assoc-in [:probe-events (.-probe_key probe-ack-event)] probe-ack-event))


