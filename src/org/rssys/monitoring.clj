(ns org.rssys.monitoring
  (:require
    [prometheus.core :as prometheus]))


(def registry (prometheus/new-registry))

(prometheus/register-metric-meta registry :ping-ack-round-trip-ms :buckets [0 5 30 50 100 200 500 1000 2000 3000 10000 20000])


