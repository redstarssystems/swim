(ns user
  (:require
    [bogus.core]
    [clojure.stacktrace :as trace]
    [hashp.core]
    [puget.printer :refer [cprint] :rename {cprint cprn}]))


(defn t-prn>
  [v]
  (when (:org.rssys.swim/cmd v)
    (cprn v)
    (cprn "-----------------------------------------------------")))


(defn run-dev
  []
  (add-tap t-prn>))


(comment
  (set! *warn-on-reflection* true)
  (run-dev)
  (add-tap (bound-fn* puget.printer/cprint))
  (tap> {:org.rssys.swim/cmd true :a 1 :b "2"})

  (remove-tap t-prn>)
  )



