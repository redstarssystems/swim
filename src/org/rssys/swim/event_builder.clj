(ns org.rssys.swim.event-builder
  (:require [clojure.set :as cs]
            [org.rssys.swim.event :as event]
            [org.rssys.swim.neighbour :as neighbour]
            [org.rssys.swim.node :as node])
  (:import (java.util UUID)
           (org.rssys.swim.event AckEvent AliveEvent AntiEntropy DeadEvent ISwimEvent IndirectAckEvent
                                 IndirectPingEvent JoinEvent LeftEvent NewClusterSizeEvent PayloadEvent
                                 PingEvent ProbeAckEvent ProbeEvent SuspectEvent)
           (org.rssys.swim.neighbour NeighbourNode)
           (org.rssys.swim.node NodeObject)))


(defn new-probe-event
  "Returns new probe event. Increase tx of `this` node."
  ^ProbeEvent [^NodeObject this ^String neighbour-host ^long neighbour-port]
  (let [probe-event (event/map->ProbeEvent {:event-code      (:probe event/code)
                                            :id              (node/get-id this)
                                            :host            (node/get-host this)
                                            :port            (node/get-port this)
                                            :restart-counter (node/get-restart-counter this)
                                            :tx              (node/get-tx this)
                                            :neighbour-host  neighbour-host
                                            :neighbour-port  neighbour-port
                                            :probe-key       (UUID/randomUUID)})]
    (node/inc-tx this)
    probe-event))


(defn new-probe-ack-event
  "Returns new probe ack event. Increase tx of `this` node."
  ^ProbeAckEvent [^NodeObject this ^ProbeEvent e]
  (let [ack-event (event/map->ProbeAckEvent {:event-code      (:probe-ack event/code)
                                             :id              (node/get-id this)
                                             :host            (node/get-host this)
                                             :port            (node/get-port this)
                                             :status          (node/get-status this)
                                             :restart-counter (node/get-restart-counter this)
                                             :tx              (node/get-tx this)
                                             :neighbour-id    (.-id e)
                                             :neighbour-tx    (.-tx e)
                                             :probe-key       (.-probe_key e)})]
    (node/inc-tx this)
    ack-event))


(defn new-ping-event
  "Returns new ping event. Increase tx of `this` node."
  ^PingEvent [^NodeObject this ^UUID neighbour-id attempt-number]
  (let [ping-event (event/map->PingEvent {:event-code      (:ping event/code)
                                          :id              (node/get-id this)
                                          :host            (node/get-host this)
                                          :port            (node/get-port this)
                                          :restart-counter (node/get-restart-counter this)
                                          :tx              (node/get-tx this)
                                          :neighbour-id    neighbour-id
                                          :attempt-number  attempt-number
                                          :ts              (System/currentTimeMillis)})]
    (node/inc-tx this)
    ping-event))


(defn new-ack-event
  "Returns new Ack event.
  Increase tx of `this` node."
  ^AckEvent [^NodeObject this ^ISwimEvent e]
  (let [ack-event (event/map->AckEvent {:event-code      (:ack event/code)
                                        :id              (node/get-id this)
                                        :restart-counter (node/get-restart-counter this)
                                        :tx              (node/get-tx this)
                                        :neighbour-id    (:id e)
                                        :neighbour-tx    (:tx e)
                                        :attempt-number  (:attempt-number e)
                                        :ts              (:ts e)})]
    (node/inc-tx this)
    ack-event))



(defn new-indirect-ping-event
  "Returns new indirect ping event. Increase tx of `this` node."
  ^IndirectPingEvent [^NodeObject this ^UUID intermediate-id ^UUID neighbour-id attempt-number]
  (let [intermediate (neighbour/get-neighbour this intermediate-id)
        neighbour    (neighbour/get-neighbour this neighbour-id)]
    (cond

      (nil? intermediate)
      (throw (ex-info "Unknown intermediate node with such id" {:intermediate-id intermediate-id}))

      (nil? neighbour)
      (throw (ex-info "Unknown neighbour node with such id" {:neighbour-id neighbour-id}))

      :else
      (let [indirect-ping-event
            (event/map->IndirectPingEvent {:event-code        (:indirect-ping event/code)
                                           :id                (node/get-id this)
                                           :host              (node/get-host this)
                                           :port              (node/get-port this)
                                           :restart-counter   (node/get-restart-counter this)
                                           :tx                (node/get-tx this)
                                           :intermediate-id   intermediate-id
                                           :intermediate-host (:host intermediate)
                                           :intermediate-port (:port intermediate)
                                           :neighbour-id      neighbour-id
                                           :neighbour-host    (:host neighbour)
                                           :neighbour-port    (:port neighbour)
                                           :attempt-number    attempt-number
                                           :ts                (System/currentTimeMillis)})]
        (node/inc-tx this)
        indirect-ping-event))))



(defn new-indirect-ack-event
  "Returns new indirect ack event. Increase tx of `this` node."
  ^IndirectAckEvent [^NodeObject this ^IndirectPingEvent e]
  (let [indirect-ack-event
        (event/map->IndirectAckEvent {:event-code        (:indirect-ack event/code)
                                      :id                (node/get-id this)
                                      :host              (node/get-host this)
                                      :port              (node/get-port this)
                                      :restart-counter   (node/get-restart-counter this)
                                      :tx                (node/get-tx this)
                                      :status            (node/get-status this)
                                      :intermediate-id   (.-intermediate_id e)
                                      :intermediate-host (.-intermediate_host e)
                                      :intermediate-port (.-intermediate_port e)
                                      :neighbour-id      (.-id e)
                                      :neighbour-host    (.-host e)
                                      :neighbour-port    (.-port e)
                                      :attempt-number    (.-attempt_number e)
                                      :ts                (.-ts e)})]
    (node/inc-tx this)
    indirect-ack-event))



(defn new-alive-event
  "Returns new Alive event. Increase tx of `this` node."
  ^AliveEvent [^NodeObject this ^ISwimEvent e]
  (let [nb (neighbour/get-neighbour this (:id e))
        alive-event
           (event/map->AliveEvent {:event-code                (:alive event/code)
                                   :id                        (node/get-id this)
                                   :restart-counter           (node/get-restart-counter this)
                                   :tx                        (node/get-tx this)
                                   :neighbour-id              (:id e)
                                   :neighbour-restart-counter (:restart-counter e)
                                   :neighbour-tx              (:tx e)
                                   :neighbour-host            (:host nb)
                                   :neighbour-port            (:port nb)})]
    (node/inc-tx this)
    alive-event))



(defn new-dead-event
  "Returns new dead event. Increase tx of `this` node."
  (^DeadEvent [^NodeObject this ^UUID neighbour-id]
   (let [nb                 (neighbour/get-neighbour this neighbour-id)
         nb-restart-counter (:restart-counter nb)
         nb-tx              (or (-> nb :events-tx (get (:ping event/code))) 0)]
     (new-dead-event this neighbour-id nb-restart-counter nb-tx)))

  (^DeadEvent [^NodeObject this neighbour-id neighbour-restart-counter neighbour-tx]
   (let [dead-event
         (event/map->DeadEvent {:event-code                (:dead event/code)
                                :id                        (node/get-id this)
                                :restart-counter           (node/get-restart-counter this)
                                :tx                        (node/get-tx this)
                                :neighbour-id              neighbour-id
                                :neighbour-restart-counter neighbour-restart-counter
                                :neighbour-tx              neighbour-tx})]
     (node/inc-tx this)
     dead-event)))




(defn neighbour->vec
  "Convert NeighbourNode to vector of values.
  Field :updated-at is omitted.
  Returns vector of values"
  [^NeighbourNode nb]
  (when (instance? NeighbourNode nb)
    [(:id nb)
     (:host nb)
     (:port nb)
     ((:status nb) event/code)
     (if (= :direct (:access nb)) 0 1)
     (:restart-counter nb)
     (:events-tx nb)
     (:payload nb)]))


(defn vec->neighbour
  "Convert vector of values to NeighbourNode.
  Returns ^NeighbourNode."
  ^NeighbourNode [v]
  (if (= 8 (count v))
    (neighbour/new-neighbour
      {:id              (nth v 0)
       :host            (nth v 1)
       :port            (nth v 2)
       :status          (get (cs/map-invert event/code) (nth v 3))
       :access          (if (zero? (nth v 4)) :direct :indirect)
       :restart-counter (nth v 5)
       :events-tx       (nth v 6)
       :payload         (nth v 7)
       :updated-at      0})
    (throw (ex-info "Invalid data in vector for NeighbourNode" {:v v}))))


(defn build-anti-entropy-data
  "Build anti-entropy data – subset of known nodes from neighbours map.
  This data is propagated from node to node and thus nodes can get knowledge about unknown nodes.
  To apply anti-entropy data receiver should compare incarnation pair [restart-counter tx] and apply only
  if node has older data.
  Returns vector of known neighbors size of `num` if any or empty vector.
  Any item in returned vector is vectorized NeighbourNode.
  If key :neighbour-id present then returns anti-entropy data for this neighbour only"
  [^NodeObject this & {:keys [num neighbour-id]}]
  (let [config (node/get-config this)
        num'   (or num (:max-anti-entropy-items config))]
    (if neighbour-id
     (if-let [ae-data (neighbour->vec (neighbour/get-neighbour this neighbour-id))]
       [ae-data]
       [])
     (or
       (some->>
         (node/get-neighbours this)
         vals
         shuffle
         (take num')
         (map neighbour->vec)
         vec)
       []))))


(defn new-anti-entropy-event
  "Returns anti-entropy event. Increase tx of `this` node.
  If key `:neighbour-id` present in `ks` then returns anti-entropy data for this neighbour only.
  If key `:num` present in `ks` then returns anti-entropy data for given number of neighbours.
  Default value for `:num` in `ks` is :max-anti-entropy-items"
  ^AntiEntropy [^NodeObject this & {:keys [] :as ks}]
  (let [anti-entropy-data (build-anti-entropy-data this ks)
        ae-event          (event/map->AntiEntropy
                            {:event-code        (:anti-entropy event/code)
                             :id                (node/get-id this)
                             :restart-counter   (node/get-restart-counter this)
                             :tx                (node/get-tx this)
                             :anti-entropy-data anti-entropy-data})]
    (node/inc-tx this)
    ae-event))



(defn new-cluster-size-event
  "Returns new NewClusterSizeEvent event. Increase tx of `this` node.
  This event should be created before cluster size changed."
  ^NewClusterSizeEvent [^NodeObject this ^long new-cluster-size]
  (let [ncs-event (event/map->NewClusterSizeEvent
                    {:event-code       (:new-cluster-size event/code)
                     :id               (node/get-id this)
                     :restart-counter  (node/get-restart-counter this)
                     :tx               (node/get-tx this)
                     :old-cluster-size (node/get-cluster-size this)
                     :new-cluster-size new-cluster-size})]
    (node/inc-tx this)
    ncs-event))



(defn new-join-event
  "Returns new JoinEvent event. Increase tx of `this` node."
  ^JoinEvent [^NodeObject this]
  (let [join-event (event/map->JoinEvent
                     {:event-code      (:join event/code)
                      :id              (node/get-id this)
                      :restart-counter (node/get-restart-counter this)
                      :tx              (node/get-tx this)
                      :host            (node/get-host this)
                      :port            (node/get-port this)})]
    (node/inc-tx this)
    join-event))




(defn new-suspect-event
  "Returns new suspect event. Increase tx of `this` node."
  ^SuspectEvent [^NodeObject this ^UUID neighbour-id]
  (let [nb                 (neighbour/get-neighbour this neighbour-id)
        nb-restart-counter (:restart-counter nb)
        nb-tx              (:tx nb)
        suspect-event      (event/map->SuspectEvent
                             {:event-code                (:suspect event/code)
                              :id                        (node/get-id this)
                              :restart-counter           (node/get-restart-counter this)
                              :tx                        (node/get-tx this)
                              :neighbour-id              neighbour-id
                              :neighbour-restart-counter nb-restart-counter
                              :neighbour-tx              nb-tx})]
    (node/inc-tx this)
    suspect-event))




(defn new-left-event
  "Returns new LeftEvent event. Increase tx of `this` node."
  ^LeftEvent [^NodeObject this]
  (let [e (event/map->LeftEvent
            {:event-code      (:left event/code)
             :id              (node/get-id this)
             :restart-counter (node/get-restart-counter this)
             :tx              (node/get-tx this)})]
    (node/inc-tx this)
    e))



(defn new-payload-event
  "Returns new PayloadEvent event. Increase tx of `this` node."
  ^PayloadEvent [^NodeObject this]
  (let [e (event/map->PayloadEvent
            {:event-code      (:payload event/code)
             :id              (node/get-id this)
             :restart-counter (node/get-restart-counter this)
             :tx              (node/get-tx this)
             :payload         (node/get-payload this)})]
    (node/inc-tx this)
    e))

