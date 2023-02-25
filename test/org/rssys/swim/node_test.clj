(ns org.rssys.swim.node-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [org.rssys.swim.node :as sut]
            [org.rssys.swim.cluster :as cluster]
            [matcho.core :as m])
  (:import (org.rssys.swim.node NodeObject)))


(def cluster-data
  {:id           #uuid "10000000-0000-0000-0000-000000000000"
   :name         "cluster1"
   :desc         "Test cluster1"
   :password     "0123456789abcdef0123456789abcdef"
   :namespace    "test-ns1"
   :cluster-size 3
   :tags         #{"dc1" "rssys"}})


(deftest new-node-object-test

  (testing "node default parameters"
    (let [test-cluster      (cluster/new-cluster cluster-data)
          node-data {:cluster test-cluster :host "127.0.0.1" :port 5376}
          this              (sut/new-node-object node-data)]

     (testing "should return NodeObject instance"
       (is (instance? NodeObject this)))

     (testing "config should contain `default-config` value"
       (m/assert ^:matcho/strict
         sut/default-config
         (sut/get-config this)))

     (testing "id should have generated uuid"
       (m/assert uuid? (sut/get-id this)))

     (testing "restart counter should have generated value of current milliseconds"
       (m/assert pos-int? (sut/get-restart-counter this)))

     (testing "neighbours should have empty map"
       (m/assert ^:matcho/strict {} (sut/get-neighbours this)))

     (testing "status should have stop value"
       (m/assert :stop (sut/get-status this)))

     (testing "tx should have zero value"
       (m/assert 0 (sut/get-tx this)))

     (testing "ping events should have empty map"
       (m/assert ^:matcho/strict {} (sut/get-ping-events this)))

     (testing "indirect ping events should have empty map"
       (m/assert ^:matcho/strict {} (sut/get-indirect-ping-events this)))

     (testing "payload should have empty map"
       (m/assert ^:matcho/strict {} (sut/get-payload this)))

     (testing "UDP server should have nil value"
       (m/assert nil? (sut/get-udp-server this)))

     (testing "outgoing events should have empty vector"
       (m/assert ^:matcho/strict [] (sut/get-outgoing-events this)))

     (testing "ping round buffer should have empty vector"
       (m/assert ^:matcho/strict [] (sut/get-ping-round-buffer this)))

     (testing "probe events should have empty map"
       (m/assert ^:matcho/strict {} (sut/get-probe-events this)))))


  (testing "override node parameters"
    (let [test-cluster (cluster/new-cluster cluster-data)
          config       (assoc sut/default-config :max-payload-size 512)
          node-data    {:config          config
                        :id              #uuid "10000000-0000-0000-0000-000000000000"
                        :cluster         test-cluster
                        :host            "127.0.0.1"
                        :port            5376
                        :restart-counter 7}
          this         (sut/new-node-object node-data)]

      (testing "should contain particular config value"
        (m/assert ^:matcho/strict
          config
          (sut/get-config this)))

      (testing "id should have particular id"
        (m/assert (:id node-data) (sut/get-id this)))

      (testing "id should have particular cluster"
        (m/assert test-cluster (sut/get-cluster this)))

      (testing "restart counter should have particular value"
        (m/assert (:restart-counter node-data) (sut/get-restart-counter this)))

      (testing "neighbours should have empty map"
        (m/assert ^:matcho/strict {} (sut/get-neighbours this)))

      (testing "host should have particular value"
        (m/assert (:host node-data) (sut/get-host this)))

      (testing "port should have particular value"
        (m/assert (:port node-data) (sut/get-port this))))))




