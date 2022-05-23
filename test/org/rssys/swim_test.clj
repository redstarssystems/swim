(ns org.rssys.swim-test
  (:require [clojure.test :as test :refer [deftest testing is]]
            [org.rssys.swim :as sut]
            [matcho.core :refer [match]]))


(deftest new-cluster-test
  (let [cluster-data {:id          #uuid "f876678d-f544-4fb8-a848-dc2c863aba6b"
                      :name        "cluster1"
                      :description "Test cluster1"
                      :secret-key  "12345678"
                      :root-nodes  [{:host "1.1.1.1" :port 1234} {:host "2.2.2.2" :port 5678}]
                      :nspace      "ns1"
                      :tags ["dc1" "rssys"]}
        result (sut/new-cluster cluster-data)]
    (is (instance? org.rssys.swim.Cluster result))
    (is (= #{:id :name :description :secret-key :root-nodes :nspace :tags})
      (keys result))))
