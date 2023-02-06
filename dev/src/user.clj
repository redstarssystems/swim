(ns user
  (:require
    [clojure.pprint :refer [pprint]]
    [clojure.string :as string]
    [hashp.core]
    [puget.printer :refer [cprint] :rename {cprint cprn}])
  (:import (java.time Instant LocalDateTime ZoneId)))

(defn print-v
  [v]
  (cprn (update v :ts (fn [ts] (-> (Instant/ofEpochMilli ts) (LocalDateTime/ofInstant (ZoneId/of "Europe/Moscow")) str))))
  (cprn "-----------------------------------------------------"))

(defn about-neighbour-prn>
  [v]
  (let [neighbour-id #uuid"00000000-0000-0000-0000-000000000002"]
    (when (and (:org.rssys.swim.core/cmd v)
            (string/includes? (str v) (str neighbour-id)))
      (when (#{:alive-event :put-event :left-event :dead-event}
                 (:org.rssys.swim.core/cmd v))
        (print-v v)))))

(defn t-prn>
  [v]
  (when (:org.rssys.swim.core/cmd v)
    (print-v v)))


(defn node-id-prn>
  [v]
  (when (and (:org.rssys.swim.core/cmd v) (= #uuid"00000000-0000-0000-0000-000000000074" (:node-id v)))
    (print-v v)))


(defn filtered-prn>
  [v]
  (when-not (#{:udp-packet-processor :upsert-neighbour :send-events-udp-size
               :new-cluster-size-event :ack-event :ping-event :insert-ping :ping-heartbeat :delete-ping
               :new-node-object :start :set-cluster-size :set-restart-counter :set-status :anti-entropy-event
               :ack-timeout}
              (:org.rssys.swim.core/cmd v))
    (print-v v)))


(def *max-ping-ack-round-trip (atom 0))


(defn ping-ack-round-trip>
  [v]

  (when (#{:ping-ack-round-trip} (:org.rssys.swim.core/cmd v))
    (when (> (:data v) @*max-ping-ack-round-trip)
      (reset! *max-ping-ack-round-trip (:data v)))

    (print-v v)))


(defn file-prn>
  [v]
  (when (:org.rssys.swim.core/cmd v)
    (let [fname (str "log/" (:node-id v) ".txt")
          content (with-out-str (pprint v))]
      (spit fname content :append true)
      (spit fname "\n-----------------------------------------------------\n" :append true))))


(defn run-dev
  []
  (add-tap t-prn>))


(comment
  (set! *warn-on-reflection* true)
  (run-dev)
  (add-tap (bound-fn* puget.printer/cprint))
  (tap> {:org.rssys.swim.core/cmd true :a 1 :b "2"})

  (add-tap about-neighbour-prn>)
  (add-tap ping-ack-round-trip>)
  (add-tap t-prn>)
  (add-tap filtered-prn>)

  (add-tap file-prn>)
  (add-tap node-id-prn>)

  (require '[taoensso.tufte :as tufte :refer (defnp p profiled profile)])
  (tufte/add-basic-println-handler! {})

  (remove-tap about-neighbour-prn>)
  (remove-tap node-id-prn>)
  (remove-tap ping-ack-round-trip>)
  (remove-tap t-prn>)
  (remove-tap filtered-prn>)
  (remove-tap file-prn>)
  )



