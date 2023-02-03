(ns profiler
  (:require [clj-async-profiler.core :as prof]))

(comment

  (prof/start)

  (prof/stop)

  (def prof-server (prof/serve-ui 8080))
  (.stop prof-server 0)

  (clojure.reflect/reflect prof-server))
