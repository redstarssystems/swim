(ns user
  (:require
    [bogus.core]
    [clojure.pprint :refer [pprint]]
    [hashp.core]
    [puget.printer :refer [cprint] :rename {cprint cprn}]))


(defn t-prn>
  [v]
  (when (:org.rssys.swim/cmd v)
    (cprn v)
    (cprn "-----------------------------------------------------")))


(defn filtered-prn>
  [v]
  (when-not (#{:udp-packet-processor :upsert-neighbour :send-events-udp-size}
              (:org.rssys.swim/cmd v))
    (cprn v)
    (cprn "-----------------------------------------------------")))


(def *max-ping-ack-round-trip (atom 0))

(defn ping-ack-round-trip>
  [v]

  (when (#{:ping-ack-round-trip} (:org.rssys.swim/cmd v))
    (when (> (:data v) @*max-ping-ack-round-trip)
      (reset! *max-ping-ack-round-trip (:data v)))

    (cprn v)
    (cprn "-----------------------------------------------------")))

(defn file-prn>
  [v]
  (when (:org.rssys.swim/cmd v)
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
  (tap> {:org.rssys.swim/cmd true :a 1 :b "2"})

  (add-tap ping-ack-round-trip>)
  (add-tap t-prn>)
  (add-tap filtered-prn>)

  (add-tap file-prn>)

  (require '[taoensso.tufte :as tufte :refer (defnp p profiled profile)])
  (tufte/add-basic-println-handler! {})

  (remove-tap ping-ack-round-trip>)
  (remove-tap t-prn>)
  (remove-tap filtered-prn>)
  (remove-tap file-prn>)
  )



