(ns web
  (:require [org.rssys.swim.metric :as metric]
            [ring.adapter.jetty :as jetty]))


(defn metrics-handler
  "Handler for scrapping metrics by Prometheus.
   Returns `Ring response` with plain text metrics in Prometheus format."
  [_]
  {:status 200
   :headers {"Content-Type" "text/plain"}
   :body (metric/serialize metric/registry)})


(comment

  (def server (jetty/run-jetty metrics-handler {:port 3000 :join? false}))

  (.stop server))
