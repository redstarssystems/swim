(ns org.rssys.event
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



(defrecord ProbeEvent [cmd-type id host port restart-counter tx neighbour-host neighbour-port probe-key]

           ISwimEvent

           (prepare [^ProbeEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e)
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
  (map->ProbeEvent {:cmd-type        (:probe code)
                    :id              (UUID. 0 0)
                    :host            "localhost"
                    :port            1
                    :restart-counter 0
                    :tx              0
                    :neighbour-host  "localhost"
                    :neighbour-port  1
                    :probe-key       (UUID. 9 1)}))


;;;;


(defrecord ProbeAckEvent [cmd-type id host port status restart-counter tx neighbour-id probe-key]

           ISwimEvent

           (prepare [^ProbeAckEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-status e) (.-restart_counter e)
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
  (map->ProbeAckEvent {:cmd-type        (:probe-ack code)
                       :id              (UUID. 0 0)
                       :host            "localhost"
                       :port            1
                       :status          :unknown
                       :restart-counter 0
                       :tx              0
                       :neighbour-id    (UUID. 0 1)
                       :probe-key       "probekey0"}))


;;;;


(defrecord PingEvent [cmd-type id host port restart-counter tx neighbour-id attempt-number]

           ISwimEvent

           (prepare [^PingEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e)
              (.-neighbour_id e) (.-attempt_number e)])

           (restore [^PingEvent _ v]
             (if (and
                   (vector? v)
                   (= 8 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ping code)) uuid? string? pos-int? nat-int?
                                                nat-int? uuid? pos-int?] v)))
               (apply ->PingEvent v)
               (throw (ex-info "PingEvent vector has invalid structure" {:ping-vec v})))))


(defn empty-ping
  "Returns empty ping event"
  ^PingEvent []
  (map->PingEvent {:cmd-type        (:ping code)
                   :id              (UUID. 0 0)
                   :host            "localhost"
                   :port            1
                   :restart-counter 0
                   :tx              0
                   :neighbour-id    (UUID. 0 1)
                   :attempt-number  1}))

;;;;

(defrecord AckEvent [cmd-type id restart-counter tx neighbour-id neighbour-tx attempt-number]

           ISwimEvent

           (prepare [^AckEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e)
              (.-neighbour_id e) (.-neighbour_tx e) (.-attempt_number e)])

           (restore [^AckEvent _ v]
             (if (and
                   (vector? v)
                   (= 7 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ack code)) uuid? nat-int? nat-int?
                                                uuid? nat-int? pos-int?] v)))
               (apply ->AckEvent v)
               (throw (ex-info "AckEvent vector has invalid structure" {:ack-vec v})))))


(defn empty-ack
  "Returns empty ack event"
  ^AckEvent []
  (map->AckEvent {:cmd-type        (:ack code)
                  :id              (UUID. 0 0)
                  :restart-counter 0
                  :tx              0
                  :neighbour-id    (UUID. 0 1)
                  :neighbour-tx    0
                  :attempt-number  1}))


;;;;

(defrecord IndirectPingEvent [cmd-type id host port restart-counter tx
                              intermediate-id intermediate-host intermediate-port
                              neighbour-id neighbour-host neighbour-port
                              attempt-number]

           ISwimEvent

           (prepare [^IndirectPingEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e)
              (.-intermediate_id e) (.-intermediate_host e) (.-intermediate_port e)
              (.-neighbour_id e) (.-neighbour_host e) (.-neighbour_port e)
              (.-attempt_number e)])

           (restore [^IndirectPingEvent _ v]
             (if (and
                   (vector? v)
                   (= 13 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:indirect-ping code)) uuid? string? pos-int? nat-int? nat-int?
                                                uuid? string? pos-int?
                                                uuid? string? pos-int?
                                                pos-int?] v)))
               (apply ->IndirectPingEvent v)
               (throw (ex-info "IndirectPingEvent vector has invalid structure" {:indirect-ping-vec v})))))


(defn empty-indirect-ping
  "Returns empty indirect ping event"
  ^IndirectPingEvent []
  (map->IndirectPingEvent {:cmd-type          (:indirect-ping code)
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
                           :attempt-number    1}))


;;;;

(defrecord IndirectAckEvent [cmd-type id host port restart-counter tx status
                             intermediate-id intermediate-host intermediate-port
                             neighbour-id neighbour-host neighbour-port
                             attempt-number]

           ISwimEvent

           (prepare [^IndirectAckEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e) (.-status e)
              (.-intermediate_id e) (.-intermediate_host e) (.-intermediate_port e)
              (.-neighbour_id e) (.-neighbour_host e) (.-neighbour_port e)
              (.-attempt_number e)])

           (restore [^IndirectAckEvent _ v]
             (if (and
                   (vector? v)
                   (= 14 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:indirect-ack code))
                                                uuid? string? pos-int? nat-int? nat-int? keyword?
                                                uuid? string? pos-int?
                                                uuid? string? pos-int?
                                                pos-int?] v)))
               (apply ->IndirectAckEvent v)
               (throw (ex-info "IndirectAckEvent vector has invalid structure" {:indirect-ping-vec v})))))


(defn empty-indirect-ack
  "Returns empty indirect ack event"
  ^IndirectAckEvent []
  (map->IndirectAckEvent {:cmd-type          (:indirect-ack code)
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
                          :attempt-number    1}))


;;;;


(defrecord AliveEvent [cmd-type id restart-counter tx
                       neighbour-id neighbour-restart-counter neighbour-tx]

           ISwimEvent

           (prepare [^AliveEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e)
              (.-neighbour_id e) (.-neighbour_restart_counter e) (.-neighbour_tx e)])

           (restore [^AliveEvent _ v]
             (if (and
                   (vector? v)
                   (= 7 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:alive code)) uuid? nat-int? nat-int?
                                                uuid? nat-int? nat-int?] v)))
               (apply ->AliveEvent v)
               (throw (ex-info "AliveEvent vector has invalid structure" {:alive-vec v})))))


(defn empty-alive
  "Returns empty alive event"
  ^AliveEvent []
  (map->AliveEvent {:cmd-type                  (:alive code)
                    :id                        (UUID. 0 0)
                    :restart-counter           0
                    :tx                        0
                    :neighbour-id              (UUID. 0 1)
                    :neighbour-restart-counter 0
                    :neighbour-tx              0}))


;;;;

(defrecord NewClusterSizeEvent [cmd-type id restart-counter tx old-cluster-size new-cluster-size]

           ISwimEvent

           (prepare [^NewClusterSizeEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-old_cluster_size e) (.-new_cluster_size e)])

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
  (map->NewClusterSizeEvent {:cmd-type         (:new-cluster-size code)
                             :id               (UUID. 0 0)
                             :restart-counter  0
                             :tx               0
                             :old-cluster-size 1
                             :new-cluster-size 2}))

;;;;


(defrecord DeadEvent [cmd-type id restart-counter tx neighbour-id neighbour-restart-counter neighbour-tx]

           ISwimEvent

           (prepare [^DeadEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e)
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
  (map->DeadEvent {:cmd-type                  (:dead code)
                   :id                        (UUID. 0 0)
                   :restart-counter           0
                   :tx                        0
                   :neighbour-id              (UUID. 0 1)
                   :neighbour-restart-counter 0
                   :neighbour-tx              0}))


;;;;


(defrecord AntiEntropy [cmd-type id restart-counter tx anti-entropy-data]
           ISwimEvent

           (prepare [^AntiEntropy e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.-tx e) (.-anti_entropy_data e)])

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
  (map->AntiEntropy {:cmd-type          (:anti-entropy code)
                     :id                (UUID. 0 0)
                     :restart-counter   0
                     :tx                0
                     :anti-entropy-data []}))


;;;;

(defrecord JoinEvent [cmd-type id restart-counter tx]

           ISwimEvent

           (prepare [^JoinEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e)])

           (restore [^JoinEvent _ v]
             (if (and
                   (vector? v)
                   (= 4 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:join code)) uuid? nat-int? nat-int?] v)))
               (apply ->JoinEvent v)
               (throw (ex-info "JoinEvent vector has invalid structure" {:join-vec v})))))


(defn empty-join
  "Returns empty join event"
  ^JoinEvent []
  (map->JoinEvent {:cmd-type        (:join code)
                   :id              (UUID. 0 0)
                   :restart-counter 0
                   :tx              0}))

;;;;


(defrecord SuspectEvent [cmd-type id restart-counter tx neighbour-id neighbour-restart-counter neighbour-tx]

           ISwimEvent

           (prepare [^SuspectEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e)
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
  (map->SuspectEvent {:cmd-type                  (:suspect code)
                      :id                        (UUID. 0 0)
                      :restart-counter           0
                      :tx                        0
                      :neighbour-id              (UUID. 0 1)
                      :neighbour-restart-counter 0
                      :neighbour-tx              0}))


;;;;

(defrecord LeftEvent [cmd-type id restart-counter tx]

           ISwimEvent

           (prepare [^LeftEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e)])

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
  (map->LeftEvent {:cmd-type        (:left code)
                   :id              (UUID. 0 0)
                   :restart-counter 0
                   :tx              0}))


;;;;

(defrecord PayloadEvent [cmd-type id restart-counter tx payload]

           ISwimEvent

           (prepare [^PayloadEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.-tx e) (.-payload e)])

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
  (map->PayloadEvent {:cmd-type        (:payload code)
                      :id              (UUID. 0 0)
                      :restart-counter 0
                      :tx              0
                      :payload         {:name "node0"}}))

