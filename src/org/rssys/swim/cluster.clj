(ns org.rssys.swim.cluster
  (:require
    [clojure.spec.alpha :as s]
    [org.rssys.swim.encrypt :as e]
    [org.rssys.swim.spec :as spec]))

(defrecord Cluster [id name desc namespace tags secret-key cluster-size])


(defn new-cluster
  "Returns new Cluster instance."
  ^Cluster [{:keys [id name password namespace desc cluster-size tags]}]
  (when-not (s/valid? ::spec/password password)
    (throw (ex-info "Invalid cluster password" (->> password (s/explain-data ::spec/password) spec/problems))))
  (let [cluster (map->Cluster {:id           (or id (random-uuid))
                               :name         name
                               :desc         (or desc "")
                               :namespace    namespace
                               :tags         (or tags #{})
                               :secret-key   (e/gen-secret-key password)
                               :cluster-size (or cluster-size 1)})]

    (if-not (s/valid? ::spec/cluster cluster)
      (throw (ex-info "Invalid cluster data" (->> cluster (s/explain-data ::spec/cluster) spec/problems)))
      cluster)))
