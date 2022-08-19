(ns user
  (:require
    [hashp.core]
    [puget.printer]))


(defn run-common
  []
  (set! *warn-on-reflection* true)
  (add-tap (bound-fn* puget.printer/cprint)))


(comment
  (run-common))
