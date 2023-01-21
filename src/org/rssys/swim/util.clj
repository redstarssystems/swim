(ns org.rssys.swim.util
  (:require
    [org.rssys.swim.metric :as metric]))


(defmacro safe
  [& body]
  `(try
     ~@body
     (catch Exception _#)))


(defmacro exec-time
  [node-id metric-kwd expr]
  `(let [current-max# (or (metric/get-metric metric/registry ~metric-kwd {:node-id ~node-id}) 0)
         start#  (. System (nanoTime))
         return# ~expr
         finish# (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
     (when (> finish# current-max#)
       (metric/gauge metric/registry ~metric-kwd {:node-id ~node-id} finish#))
     return#))
