(ns org.rssys.event
  "SWIM protocol events"
  (:require
    [clojure.set])
  (:import
    (java.util
      UUID)))


(def code
  {:ping         0
   :ack          1
   :join         2
   :alive        3
   :suspect      4
   :left         5
   :dead         6
   :payload      7
   :anti-entropy 8
   :probe        9
   :probe-ack    10
   :stop         11
   :unknown      12})


(def ping-types {:direct 0 :indirect 1})


(defprotocol ISwimEvent
  (prepare [this] "Convert Event to vector of values for subsequent serialization")
  (restore [this v] "Restore Event from vector of values"))


(defrecord PingEvent [cmd-type id host port restart-counter tx neighbour-id attempt-number ptype]

           ISwimEvent

           (prepare [^PingEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e)
              (.-neighbour_id e) (.-attempt_number e) ((.-ptype e) ping-types)])

           (restore [^PingEvent _ v]
             (if (and
                   (vector? v)
                   (= 9 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ping code)) uuid? string? pos-int? nat-int?
                                                nat-int? uuid? pos-int? nat-int?] v)))
               (apply ->PingEvent (assoc v 8 (get (clojure.set/map-invert ping-types) (nth v 8))))
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
                   :neighbour-id    (UUID. 0 0)
                   :attempt-number  1
                   :ptype           :direct}))


;;;;

(defrecord AckEvent [cmd-type id restart-counter tx neighbour-id neighbour-tx]

           ISwimEvent

           (prepare [^AckEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e) (.-neighbour_tx e)])

           (restore [^AckEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ack code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
               (apply ->AckEvent v)
               (throw (ex-info "AckEvent vector has invalid structure" {:ack-vec v})))))


(defn empty-ack
  "Returns empty ack event"
  ^AckEvent []
  (map->AckEvent {:cmd-type        (:ack code)
                  :id              (UUID. 0 0)
                  :restart-counter 0
                  :tx              0
                  :neighbour-id    (UUID. 0 0)
                  :neighbour-tx    0}))


;;;;


(defrecord DeadEvent [cmd-type id restart-counter tx neighbour-id neighbour-tx]

           ISwimEvent

           (prepare [^DeadEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e) (.-neighbour_tx e)])

           (restore [^DeadEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:dead code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
               (apply ->DeadEvent v)
               (throw (ex-info "DeadEvent vector has invalid structure" {:dead-vec v})))))


(defn empty-dead
  "Returns empty dead event"
  ^DeadEvent []
  (map->DeadEvent {:cmd-type        (:dead code)
                   :id              (UUID. 0 0)
                   :restart-counter 0
                   :tx              0
                   :neighbour-id    (UUID. 0 0)
                   :neighbour-tx    0}))


;;;;


(defrecord ProbeEvent [cmd-type id host port restart-counter tx neighbour-host neighbour-port]

           ISwimEvent

           (prepare [^ProbeEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e) (.-neighbour_host e) (.-neighbour_port e)])

           (restore [^ProbeEvent _ v]
             (if (and
                   (vector? v)
                   (= 8 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:probe code)) uuid? string? pos-int? nat-int? nat-int? string? pos-int?] v)))
               (apply ->ProbeEvent v)
               (throw (ex-info "ProbeEvent vector has invalid structure" {:probe-vec v})))))



(defn empty-probe
  "Returns empty probe event"
  ^ProbeEvent []
  (map->ProbeEvent {:cmd-type        (:probe code)
                    :id              (UUID. 0 0)
                    :host            "localhost"
                    :port            0
                    :restart-counter 0
                    :tx              0
                    :neighbour-host  "localhost"
                    :neighbour-port  0}))


;;;;

(defrecord ProbeAckEvent [cmd-type id host port status restart-counter tx neighbour-id neighbour-tx]

           ISwimEvent

           (prepare [^ProbeAckEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) ((.-status e) code) (.-restart_counter e)
              (.tx e) (.-neighbour_id e) (.-neighbour_tx e)])

           (restore [^ProbeAckEvent _ v]
             (if (and
                   (vector? v)
                   (= 9 (count v))
                   (every? true? (map #(%1 %2)
                                   [#(= % (:probe-ack code)) uuid? string? nat-int? nat-int?
                                    nat-int? nat-int? uuid? nat-int?] v)))
               (apply ->ProbeAckEvent (assoc v 4 (get (clojure.set/map-invert code) (nth v 4))))
               (throw (ex-info "ProbeAckEvent vector has invalid structure" {:probe-ack-vec v})))))



(defn empty-probe-ack
  "Returns empty probe ack event"
  ^ProbeAckEvent []
  (map->ProbeAckEvent {:cmd-type        (:probe-ack code)
                       :id              (UUID. 0 0)
                       :host            "127.0.0.1"
                       :port            0
                       :status          :unknown
                       :restart-counter 0
                       :tx              0
                       :neighbour-id    (UUID. 0 0)
                       :neighbour-tx    0}))


;;;;


(defrecord AntiEntropy [cmd-type anti-entropy-data]
           ISwimEvent

           (prepare [^AntiEntropy e]
             [(.-cmd_type e) (.-anti_entropy_data e)])

           (restore [^AntiEntropy _ v]
             (if (and
                   (vector? v)
                   (= 2 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:anti-entropy code)) vector?] v)))
               (apply ->AntiEntropy v)
               (throw (ex-info "AntiEntropy vector has invalid structure" {:anti-entropy-vec v})))))


(defn empty-anti-entropy
  "Returns empty anti-entropy event"
  ^AntiEntropy []
  (map->AntiEntropy {:cmd-type          (:anti-entropy code)
                     :anti-entropy-data []}))


;;;;


(defrecord AliveEvent [cmd-type id restart-counter tx neighbour-id neighbour-tx]

           ISwimEvent

           (prepare [^AliveEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e) (.-neighbour_tx e)])

           (restore [^AliveEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:alive code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
               (apply ->AliveEvent v)
               (throw (ex-info "AliveEvent vector has invalid structure" {:alive-vec v})))))


(defn empty-alive
  "Returns empty alive event"
  ^AliveEvent []
  (map->AliveEvent {:cmd-type        (:alive code)
                    :id              (UUID. 0 0)
                    :restart-counter 0
                    :tx              0
                    :neighbour-id    (UUID. 0 0)
                    :neighbour-tx    0}))


