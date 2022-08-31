(ns org.rssys.event
  "SWIM protocol events"
  (:import (java.util UUID)))


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
   :probe-ack    10})


(defprotocol ISwimEvent
  (prepare [this] "Convert Event to vector of values for subsequent serialization")
  (restore [this v] "Restore Event from vector of values"))


(defrecord PingEvent [cmd-type id host port restart-counter tx neighbour-id attempt-number]

           ISwimEvent

           (prepare [^PingEvent e]
             [(.-cmd_type e) (.-id e) (.-host e) (.-port e) (.-restart_counter e) (.-tx e) (.-neighbour_id e) (.-attempt_number e)])

           (restore [^PingEvent _ v]
             (if (and
                   (vector? v)
                   (= 8 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:ping code)) uuid? string? pos-int? nat-int? nat-int? uuid? pos-int?] v)))
               (apply ->PingEvent v)
               (throw (ex-info "PingEvent vector has invalid structure" {:ping-vec v})))))


(defn empty-ping
  "Returns empty ping event"
  ^PingEvent []
  (map->PingEvent {:cmd-type        (:ping code)
                   :id              (UUID. 0 0)
                   :host            "localhost"
                   :port            0
                   :restart-counter 0
                   :tx              0
                   :neighbour-id    (UUID. 0 0)
                   :attempt-number  1}))

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


;;;;

(defrecord ProbeAckEvent [cmd-type id restart-counter tx neighbour-id neighbour-tx]

           ISwimEvent

           (prepare [^ProbeAckEvent e]
             [(.-cmd_type e) (.-id e) (.-restart_counter e) (.tx e) (.-neighbour_id e) (.-neighbour_tx e)])

           (restore [^ProbeAckEvent _ v]
             (if (and
                   (vector? v)
                   (= 6 (count v))
                   (every? true? (map #(%1 %2) [#(= % (:probe-ack code)) uuid? nat-int? nat-int? uuid? nat-int?] v)))
               (apply ->ProbeAckEvent v)
               (throw (ex-info "ProbeAckEvent vector has invalid structure" {:probe-ack-vec v})))))


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

;;;;

