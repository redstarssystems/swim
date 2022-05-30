(ns org.rssys.swim-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test :refer [deftest is testing]]
    [org.rssys.swim :as sut])
  (:import
    (clojure.lang
      Atom)
    (org.rssys.swim
      Cluster
      Node)))


(deftest new-cluster-test
  (testing "Create Cluster instance is successful"
    (let [cluster-data {:id          #uuid "f876678d-f544-4fb8-a848-dc2c863aba6b"
                        :name        "cluster1"
                        :description "Test cluster1"
                        :secret-key  "0123456789abcdef0123456789abcdef"
                        :root-nodes  [{:host "127.0.0.1" :port 5376} {:host "127.0.0.1" :port 5377}]
                        :nspace      "test-ns1"
                        :tags        ["dc1" "rssys"]}
          expected-set #{:id :name :description :secret-key :root-nodes :nspace :tags}
          result       (sut/new-cluster cluster-data)]
      (is (instance? Cluster result) "Should be Cluster instance")
      (is (= (set (keys result)) expected-set) "Key set should be as expected")
      (is (thrown-with-msg? Exception #"Cluster values should correspond to spec"
            (sut/new-cluster {:a 1}))))))


(deftest new-node-test
  (testing "Create Node instance is successful"
    (let [cluster-data {:id          #uuid "f876678d-f544-4fb8-a848-dc2c863aba6b"
                        :name        "cluster1"
                        :description "Test cluster1"
                        :secret-key  "0123456789abcdef0123456789abcdef"
                        :root-nodes  [{:host "127.0.0.1" :port 5376} {:host "127.0.0.1" :port 5377}]
                        :nspace      "test-ns1"
                        :tags        ["dc1" "rssys"]}
          cluster      (sut/new-cluster cluster-data)
          node-data    {:name "node1" :host "127.0.0.1" :port 5376 :cluster cluster :tags ["dc1" "node1"]}
          *result      (sut/new-node node-data)
          expected-set #{:id :name :host :port :cluster :continue? :status :neighbours-table :*udp-server
                         :restart-counter :scheduler-pool :tx-counter :ping-ids :ping-data :tags}]
      (is (instance? Atom *result) "Should be an Atom")
      (is (instance? Node @*result) "Atom should contain a Node instance")
      (is (= (set (keys @*result)) expected-set) "Key set in a Node instance should be as expected")
      (is (thrown-with-msg? Exception #"Node values should correspond to spec"
            (sut/new-node {:a 1}))))))




(deftest node-start-test
  (testing "Node start is successful"
    (let [cluster-data {:id          #uuid "f876678d-f544-4fb8-a848-dc2c863aba6b"
                        :name        "cluster1"
                        :description "Test cluster1"
                        :secret-key  "0123456789abcdef0123456789abcdef"
                        :root-nodes  [{:host "127.0.0.1" :port 5376} {:host "127.0.0.1" :port 5377}]
                        :nspace      "test-ns1"
                        :tags        ["dc1" "rssys"]}
          cluster      (sut/new-cluster cluster-data)
          node-data    {:name "node1" :host "127.0.0.1" :port 5376 :cluster cluster :tags ["dc1" "node1"]}
          *node1      (sut/new-node node-data)]
      (sut/node-start *node1 (fn [data] (prn "received: " (String. ^bytes data))))
      (is (s/valid? ::sut/*udp-server @(:*udp-server @*node1)) "Node should have valid UDP server structure")
      (is (#{:joining :normal} (:status @*node1)) "Node should have valid status")
      (sut/node-stop *node1))))
