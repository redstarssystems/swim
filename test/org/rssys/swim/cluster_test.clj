(ns org.rssys.swim.cluster-test
  (:require [clojure.string :as string]
            [clojure.test :refer [deftest testing]]
            [matcho.core :as m]
            [org.rssys.swim.cluster :as sut]
            [org.rssys.swim.spec :as spec])
  (:import (org.rssys.swim.cluster Cluster)))


(def cluster-data
  {:id           #uuid "10000000-0000-0000-0000-000000000000"
   :name         "cluster1"
   :desc         "Test cluster1"
   :password     "0123456789abcdef0123456789abcdef"
   :namespace    "test-ns1"
   :cluster-size 3
   :tags         #{"dc1" "rssys"}})


(deftest new-cluster-test
  (testing "Create Cluster instance"
    (let [cluster (sut/new-cluster cluster-data)]

      (testing "should prevent password serialization to string"
        (m/assert #(string/includes? % ":password \"***censored***\"") (pr-str cluster)))

      (testing "should produce correct type"
        (m/assert Cluster (type cluster)))

      (testing "should produce correct structure"
        (m/assert ::spec/cluster cluster))

      (testing "should have expected cluster size"
        (m/assert 3 (.-cluster_size cluster))))))
