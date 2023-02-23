(ns org.rssys.swim.event
  "SWIM protocol events"
  (:require
    [clojure.set])
  (:import
    (java.util
      UUID)))


(def code
  {:ping             0
   :ack              1
   :join             2
   :alive            3
   :suspect          4
   :left             5
   :dead             6
   :payload          7
   :anti-entropy     8
   :probe            9
   :probe-ack        10
   :stop             11
   :unknown          12
   :new-cluster-size 13
   :indirect-ping    14
   :indirect-ack     15})


(defprotocol ISwimEvent
  (prepare [this] "Convert Event to vector of values for subsequent serialization")
  (restore [this v] "Restore Event from vector of values"))



(defrecord ProbeEvent [event-code id host port restart-counter tx neighbour-host neighbour-port probe-key]

           ISwimEvent

           (prepare [^ProbeEvent e]
             [(.event_code e) (.-id e) (.-host e) (.-port e) (.-restart_counter e)
              (.-tx e) (.-neighbour_host e) (.-neighbour_port e) (.-probe_key e)])

           (restore [^ProbeEvent _ v]
             (if (and
                   (vector? v)
                   (= 9 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:probe code)) uuid? string? pos-int? nat-int?
                                                nat-int? string? pos-int? any?] v)))
               (apply ->ProbeEvent v)
               (throw (ex-info "ProbeEvent vector has invalid structure" {:probe-vec v})))))



(defn empty-probe
  "Returns empty probe event"
  ^ProbeEvent []
  (map->ProbeEvent {:event-code      (:probe code)
                    :id              (UUID. 0 0)
                    :host            "localhost"
                    :port            1
                    :restart-counter 0
                    :tx              0
                    :neighbour-host  "localhost"
                    :neighbour-port  1
                    :probe-key       (UUID. 9 1)}))


;;;;


(defrecord ProbeAckEvent [event-code id host port status restart-counter tx neighbour-id probe-key]

           ISwimEvent

           (prepare [^ProbeAckEvent e]
             [(.event_code e) (.-id e) (.-host e) (.-port e) (.-status e) (.-restart_counter e)
              (.tx e) (.-neighbour_id e) (.-probe_key e)])

           (restore [^ProbeAckEvent _ v]
             (if (and
                   (vector? v)
                   (= 9 (count v))
                   (every? true? (map #(%1 %2)
                                   [#(= % (:probe-ack code)) uuid? string? pos-int? keyword?
                                    nat-int? nat-int? uuid? any?] v)))
               (apply ->ProbeAckEvent v)
               (throw (ex-info "ProbeAckEvent vector has invalid structure" {:probe-ack-vec v})))))



(defn empty-probe-ack
  "Returns empty probe ack event"
  ^ProbeAckEvent []
  (map->ProbeAckEvent {:event-code      (:probe-ack code)
                       :id              (UUID. 0 0)
                       :host            "localhost"
                       :port            1
                       :status          :unknown
                       :restart-counter 0
                       :tx              0
                       :neighbour-id    (UUID. 0 1)
                       :probe-key       "probekey0"}))


;;;;


(defrecord PingEvent [event-code id host port restart-counter tx neighbour-id attempt-number ts]

           ISwimEvent

           (prepare [^PingEvent e]
             [(.event_code e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e)
              (.-neighbour_id e) (.-attempt_number e) (.-ts e)])

           (restore [^PingEvent _ v]
             (if (and
                   (vector? v)
                   (= 9 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ping code)) uuid? string? pos-int? nat-int?
                                                nat-int? uuid? pos-int? pos-int?] v)))
               (apply ->PingEvent v)
               (throw (ex-info "PingEvent vector has invalid structure" {:ping-vec v})))))


(defn empty-ping
  "Returns empty ping event"
  ^PingEvent []
  (map->PingEvent {:event-code      (:ping code)
                   :id              (UUID. 0 0)
                   :host            "localhost"
                   :port            1
                   :restart-counter 0
                   :tx              0
                   :neighbour-id    (UUID. 0 1)
                   :attempt-number  1
                   :ts              1}))


;;;;

(defrecord AckEvent [event-code id restart-counter tx neighbour-id neighbour-tx attempt-number ts]

           ISwimEvent

           (prepare [^AckEvent e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.tx e)
              (.-neighbour_id e) (.-neighbour_tx e) (.-attempt_number e) (.-ts e)])

           (restore [^AckEvent _ v]
             (if (and
                   (vector? v)
                   (= 8 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ack code)) uuid? nat-int? nat-int?
                                                uuid? nat-int? pos-int? pos-int?] v)))
               (apply ->AckEvent v)
               (throw (ex-info "AckEvent vector has invalid structure" {:ack-vec v})))))


(defn empty-ack
  "Returns empty ack event"
  ^AckEvent []
  (map->AckEvent {:event-code      (:ack code)
                  :id              (UUID. 0 0)
                  :restart-counter 0
                  :tx              0
                  :neighbour-id    (UUID. 0 1)
                  :neighbour-tx    0
                  :attempt-number  1
                  :ts              1}))


;;;;

(defrecord IndirectPingEvent [event-code id host port restart-counter tx
                              intermediate-id intermediate-host intermediate-port
                              neighbour-id neighbour-host neighbour-port
                              attempt-number ts]

           ISwimEvent

           (prepare [^IndirectPingEvent e]
             [(.event_code e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e)
              (.-intermediate_id e) (.-intermediate_host e) (.-intermediate_port e)
              (.-neighbour_id e) (.-neighbour_host e) (.-neighbour_port e)
              (.-attempt_number e) (.-ts e)])

           (restore [^IndirectPingEvent _ v]
             (if (and
                   (vector? v)
                   (= 14 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:indirect-ping code)) uuid? string? pos-int? nat-int? nat-int?
                                                uuid? string? pos-int?
                                                uuid? string? pos-int?
                                                pos-int? pos-int?] v)))
               (apply ->IndirectPingEvent v)
               (throw (ex-info "IndirectPingEvent vector has invalid structure" {:indirect-ping-vec v})))))


(defn empty-indirect-ping
  "Returns empty indirect ping event"
  ^IndirectPingEvent []
  (map->IndirectPingEvent {:event-code        (:indirect-ping code)
                           :id                (UUID. 0 0)
                           :host              "localhost"
                           :port              1
                           :restart-counter   0
                           :tx                0
                           :intermediate-id   (UUID. 0 1)
                           :intermediate-host "localhost"
                           :intermediate-port 10
                           :neighbour-id      (UUID. 0 2)
                           :neighbour-host    "localhost"
                           :neighbour-port    100
                           :attempt-number    1
                           :ts                1}))


;;;;

(defrecord IndirectAckEvent [event-code id host port restart-counter tx status
                             intermediate-id intermediate-host intermediate-port
                             neighbour-id neighbour-host neighbour-port
                             attempt-number ts]

           ISwimEvent

           (prepare [^IndirectAckEvent e]
             [(.event_code e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e) (.-status e)
              (.-intermediate_id e) (.-intermediate_host e) (.-intermediate_port e)
              (.-neighbour_id e) (.-neighbour_host e) (.-neighbour_port e)
              (.-attempt_number e) (.-ts e)])

           (restore [^IndirectAckEvent _ v]
             (if (and
                   (vector? v)
                   (= 15 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:indirect-ack code))
                                                uuid? string? pos-int? nat-int? nat-int? keyword?
                                                uuid? string? pos-int?
                                                uuid? string? pos-int?
                                                pos-int? pos-int?] v)))
               (apply ->IndirectAckEvent v)
               (throw (ex-info "IndirectAckEvent vector has invalid structure" {:indirect-ping-vec v})))))


(defn empty-indirect-ack
  "Returns empty indirect ack event"
  ^IndirectAckEvent []
  (map->IndirectAckEvent {:event-code        (:indirect-ack code)
                          :id                (UUID. 0 0)
                          :host              "localhost"
                          :port              1
                          :restart-counter   0
                          :tx                0
                          :status            :unknown
                          :intermediate-id   (UUID. 0 1)
                          :intermediate-host "localhost"
                          :intermediate-port 10
                          :neighbour-id      (UUID. 0 2)
                          :neighbour-host    "localhost"
                          :neighbour-port    100
                          :attempt-number    1
                          :ts                1}))


;;;;


(defrecord AliveEvent [event-code id restart-counter tx
                       neighbour-id neighbour-restart-counter neighbour-tx
                       neighbour-host neighbour-port]

           ISwimEvent

           (prepare [^AliveEvent e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.tx e)
              (.-neighbour_id e) (.-neighbour_restart_counter e) (.-neighbour_tx e)
              (.-neighbour_host e) (.-neighbour_port e)])

           (restore [^AliveEvent _ v]
             (if (and
                   (vector? v)
                   (= 9 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:alive code)) uuid? nat-int? nat-int?
                                                uuid? nat-int? nat-int? string? pos-int?] v)))
               (apply ->AliveEvent v)
               (throw (ex-info "AliveEvent vector has invalid structure" {:alive-vec v})))))


(defn empty-alive
  "Returns empty alive event"
  ^AliveEvent []
  (map->AliveEvent {:event-code                (:alive code)
                    :id                        (UUID. 0 0)
                    :restart-counter           0
                    :tx                        0
                    :neighbour-id              (UUID. 0 1)
                    :neighbour-restart-counter 0
                    :neighbour-tx              0
                    :neighbour-host            "localhost"
                    :neighbour-port            1}))


;;;;

(defrecord NewClusterSizeEvent [event-code id restart-counter tx old-cluster-size new-cluster-size]

           ISwimEvent

           (prepare [^NewClusterSizeEvent e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.tx e) (.-old_cluster_size e) (.-new_cluster_size e)])

           (restore [^NewClusterSizeEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:new-cluster-size code)) uuid? nat-int? nat-int? nat-int? nat-int?] v)))
               (apply ->NewClusterSizeEvent v)
               (throw (ex-info "NewClusterSizeEvent vector has invalid structure" {:new-cluster-size-vec v})))))


(defn empty-new-cluster-size
  "Returns empty new cluster size event"
  ^NewClusterSizeEvent []
  (map->NewClusterSizeEvent {:event-code       (:new-cluster-size code)
                             :id               (UUID. 0 0)
                             :restart-counter  0
                             :tx               0
                             :old-cluster-size 1
                             :new-cluster-size 2}))


;;;;


(defrecord DeadEvent [event-code id restart-counter tx neighbour-id neighbour-restart-counter neighbour-tx]

           ISwimEvent

           (prepare [^DeadEvent e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e)
              (.-neighbour_restart_counter e) (.-neighbour_tx e)])

           (restore [^DeadEvent _ v]
             (if (and
                   (vector? v)
                   (= 7 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:dead code)) uuid? nat-int? nat-int? uuid? nat-int? nat-int?] v)))
               (apply ->DeadEvent v)
               (throw (ex-info "DeadEvent vector has invalid structure" {:dead-vec v})))))


(defn empty-dead
  "Returns empty dead event"
  ^DeadEvent []
  (map->DeadEvent {:event-code                (:dead code)
                   :id                        (UUID. 0 0)
                   :restart-counter           0
                   :tx                        0
                   :neighbour-id              (UUID. 0 1)
                   :neighbour-restart-counter 0
                   :neighbour-tx              0}))


;;;;


(defrecord AntiEntropy [event-code id restart-counter tx anti-entropy-data]
           ISwimEvent

           (prepare [^AntiEntropy e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.-tx e) (.-anti_entropy_data e)])

           (restore [^AntiEntropy _ v]
             (if (and
                   (vector? v)
                   (= 5 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:anti-entropy code)) uuid? nat-int? nat-int? vector?] v)))
               (apply ->AntiEntropy v)
               (throw (ex-info "AntiEntropy vector has invalid structure" {:anti-entropy-vec v})))))


(defn empty-anti-entropy
  "Returns empty anti-entropy event"
  ^AntiEntropy []
  (map->AntiEntropy {:event-code        (:anti-entropy code)
                     :id                (UUID. 0 0)
                     :restart-counter   0
                     :tx                0
                     :anti-entropy-data []}))


;;;;

(defrecord JoinEvent [event-code id restart-counter tx host port]

           ISwimEvent

           (prepare [^JoinEvent e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.tx e) (.-host e) (.-port e)])

           (restore [^JoinEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:join code)) uuid? nat-int? nat-int?
                                                string? nat-int?] v)))
               (apply ->JoinEvent v)
               (throw (ex-info "JoinEvent vector has invalid structure" {:join-vec v})))))


(defn empty-join
  "Returns empty join event"
  ^JoinEvent []
  (map->JoinEvent {:event-code      (:join code)
                   :id              (UUID. 0 0)
                   :restart-counter 0
                   :tx              0
                   :host            "localhost"
                   :port            1}))


;;;;


(defrecord SuspectEvent [event-code id restart-counter tx neighbour-id neighbour-restart-counter neighbour-tx]

           ISwimEvent

           (prepare [^SuspectEvent e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e)
              (.-neighbour_restart_counter e) (.-neighbour_tx e)])

           (restore [^SuspectEvent _ v]
             (if (and
                   (vector? v)
                   (= 7 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:suspect code)) uuid? nat-int? nat-int? uuid? nat-int? nat-int?] v)))
               (apply ->SuspectEvent v)
               (throw (ex-info "SuspectEvent vector has invalid structure" {:suspect-vec v})))))


(defn empty-suspect
  "Returns empty suspect event"
  ^SuspectEvent []
  (map->SuspectEvent {:event-code                (:suspect code)
                      :id                        (UUID. 0 0)
                      :restart-counter           0
                      :tx                        0
                      :neighbour-id              (UUID. 0 1)
                      :neighbour-restart-counter 0
                      :neighbour-tx              0}))


;;;;

(defrecord LeftEvent [event-code id restart-counter tx]

           ISwimEvent

           (prepare [^LeftEvent e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.tx e)])

           (restore [^LeftEvent _ v]
             (if (and
                   (vector? v)
                   (= 4 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:left code)) uuid? nat-int? nat-int?] v)))
               (apply ->LeftEvent v)
               (throw (ex-info "LeftEvent vector has invalid structure" {:left-vec v})))))


(defn empty-left
  "Returns empty left event"
  ^LeftEvent []
  (map->LeftEvent {:event-code      (:left code)
                   :id              (UUID. 0 0)
                   :restart-counter 0
                   :tx              0}))


;;;;

(defrecord PayloadEvent [event-code id restart-counter tx payload]

           ISwimEvent

           (prepare [^PayloadEvent e]
             [(.event_code e) (.-id e) (.-restart_counter e) (.-tx e) (.-payload e)])

           (restore [^PayloadEvent _ v]
             (if (and
                   (vector? v)
                   (= 5 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:payload code)) uuid? nat-int? nat-int? any?] v)))
               (apply ->PayloadEvent v)
               (throw (ex-info "PayloadEvent vector has invalid structure" {:payload-vec v})))))


(defn empty-payload
  "Returns empty payload event"
  ^PayloadEvent []
  (map->PayloadEvent {:event-code      (:payload code)
                      :id              (UUID. 0 0)
                      :restart-counter 0
                      :tx              0
                      :payload         {:name "node0"}}))

