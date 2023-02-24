(ns org.rssys.swim.cluster
  (:require
    [clojure.spec.alpha :as s]
    [org.rssys.swim.encrypt :as e]
    [org.rssys.swim.spec :as spec])
  (:import (java.io Writer)))


(defn cluster-str
  "Returns String representation of Cluster"
  ^String
  [cluster]
  (str (into {} (assoc cluster :password "***censored***"))))


(defrecord Cluster [id name desc password namespace tags secret-key cluster-size]
           Object
           (toString [this] (cluster-str this)))


(defmethod print-method Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


(defmethod print-dup Cluster [cluster ^Writer writer]
  (.write writer (cluster-str cluster)))


(defn new-cluster
  "Returns new Cluster instance."
  ^Cluster [{:keys [id name password namespace desc cluster-size tags] :as c}]
  (when-not (s/valid? ::spec/cluster c)
    (throw (ex-info "Invalid cluster data" (->> c (s/explain-data ::spec/cluster) spec/problems))))
  (map->Cluster {:id           (or id (random-uuid))
                 :name         name
                 :desc         (or desc "")
                 :password     password
                 :namespace    namespace
                 :tags         (or tags #{})
                 :secret-key   (e/gen-secret-key password)
                 :cluster-size (or cluster-size 1)}))
