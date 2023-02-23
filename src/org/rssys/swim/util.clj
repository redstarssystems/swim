(ns org.rssys.swim.util
  (:require
    [org.rssys.swim.metric :as metric]))


(defmacro safe
  "Execute body and suppress an Exception if occurs."
  [& body]
  `(try
     ~@body
     (catch Exception _#)))


(defmacro exec-time
  "Set max detected execution time for expression to gauge.
  Params:
  `node-id` - node id where an `expr` is executed.
  `metric-kwd` - unique id for code place where an `expr` is executed.
  `expr` - an expression to be executed."
  [node-id metric-kwd expr]
  `(let [current-max# (or (metric/get-metric metric/registry ~metric-kwd {:node-id ~node-id}) 0)
         start#  (. System (nanoTime))
         return# ~expr
         finish# (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
     (when (> finish# current-max#)
       (metric/gauge metric/registry ~metric-kwd {:node-id ~node-id} finish#))
     return#))
