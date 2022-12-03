(ns org.rssys.swim-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as string]
    [clojure.test :refer [deftest is testing]]
    [matcho.core :refer [match not-match]]
    [matcho.core :as m]
    [org.rssys.encrypt :as e]
    [org.rssys.event :as event]
    [org.rssys.spec :as spec]
    [org.rssys.swim :as sut])
  (:import
    (java.util
      UUID)
    (org.rssys.domain
      Cluster
      NeighbourNode
      NodeObject)
    (org.rssys.event
      AckEvent
      AliveEvent
      AntiEntropy
      DeadEvent
      IndirectAckEvent
      IndirectPingEvent
      JoinEvent
      LeftEvent
      NewClusterSizeEvent
      PayloadEvent
      PingEvent
      ProbeAckEvent
      ProbeEvent
      SuspectEvent)
    (org.rssys.scheduler
      MutablePool)))


(def ^:dynamic *max-test-timeout*
  "Max promise timeout ms in tests"
  1500)


(deftest safe-test
  (testing "Any Exceptions should be prevented"
    (m/assert nil (sut/safe (/ 1 0))))
  (testing "Any normal expression should be succeed"
    (m/assert 1/2 (sut/safe (/ 1 2)))))


(deftest calc-n-test
  (testing "How many nodes should we notify depending on N nodes in a cluster"
    (let [nodes-in-cluster [1 2 4 8 16 32 64 128 256 512 1024]
          result           (mapv sut/calc-n nodes-in-cluster)]
      (m/assert [0 1 2 3 4 5 6 7 8 9 10] result))))


(def values [1 128 "hello" nil :k {:a 1 :b true} [1234567890 1]])


(deftest serialize-test
  (let [svalues (->> values (map sut/serialize))]
    (is (every? bytes? svalues) "Serialized value is a bytes array")
    (is (= [6 7 11 6 9 11 7] (mapv count svalues)) "Serialized value has expected length")))


(deftest deserialize-test
  (testing "Deserialization works as expected on different types"
    (let [bvalues (map byte-array
                    '([-110 -93 126 35 39 1]
                       [-110 -93 126 35 39 -52 -128]
                       [-110 -93 126 35 39 -91 104 101 108 108 111]
                       [-110 -93 126 35 39 -64]
                       [-110 -93 126 35 39 -93 126 58 107]
                       [-126 -93 126 58 97 1 -93 126 58 98 -61]
                       [-110, -50, 73, -106, 2, -46, 1]))
          dvalues (mapv sut/deserialize bvalues)]
      (m/assert values dvalues))))


;;;;;;;;;;

(def cluster-data
  {:id           #uuid "10000000-0000-0000-0000-000000000000"
   :name         "cluster1"
   :desc         "Test cluster1"
   :secret-token "0123456789abcdef0123456789abcdef"
   :nspace       "test-ns1"
   :cluster-size 3
   :tags         #{"dc1" "rssys"}})


(def neighbour-data1
  {:id              #uuid"00000000-0000-0000-0000-000000000002"
   :host            "127.0.0.1"
   :port            5377
   :status          :alive
   :access          :direct
   :restart-counter 3
   :tx              0
   :payload         {:tcp-port 4567}
   :updated-at      (System/currentTimeMillis)})


(def neighbour-data2
  {:id              #uuid"00000000-0000-0000-0000-000000000003"
   :host            "127.0.0.1"
   :port            5378
   :status          :alive
   :access          :direct
   :restart-counter 5
   :tx              0
   :payload         {:tcp-port 4567}
   :updated-at      (System/currentTimeMillis)})


(def neighbour-data3
  {:id              #uuid"00000000-0000-0000-0000-000000000001"
   :host            "127.0.0.1"
   :port            5376
   :status          :alive
   :access          :direct
   :restart-counter 7
   :tx              0
   :payload         {:tcp-port 4567}
   :updated-at      (System/currentTimeMillis)})


(def node-data1 {:id #uuid "00000000-0000-0000-0000-000000000001" :host "127.0.0.1" :port 5376 :restart-counter 7})
(def node-data2 {:id #uuid "00000000-0000-0000-0000-000000000002" :host "127.0.0.1" :port 5377 :restart-counter 5})
(def node-data3 {:id #uuid "00000000-0000-0000-0000-000000000003" :host "127.0.0.1" :port 5378 :restart-counter 6})


;;;;


(deftest new-cluster-test
  (testing "Create Cluster instance is successful"
    (let [result (sut/new-cluster cluster-data)]
      (m/assert Cluster (type result))
      (m/assert ::spec/cluster result)
      (m/assert 3 (.-cluster_size result)))))


;;;;


(deftest new-neighbour-node-test
  (testing "Create NeighbourNode instance is successful"
    (let [result1 (sut/new-neighbour-node neighbour-data1)
          result2 (sut/new-neighbour-node #uuid "00000000-0000-0000-0000-000000000000" "127.0.0.1" 5379)]
      (m/assert NeighbourNode (type result1))
      (m/assert NeighbourNode (type result2))
      (m/assert ::spec/neighbour-node result1)
      (m/assert ::spec/neighbour-node result2)
      (testing "NeighbourNode has expected keys and values"
        (m/assert neighbour-data1 result1))
      (m/assert ^:matcho/strict
        {:id              #uuid "00000000-0000-0000-0000-000000000000"
         :host            "127.0.0.1"
         :port            5379
         :status          :unknown
         :access          :direct
         :restart-counter 0
         :tx              0
         :payload         {}
         :updated-at      nat-int?}
        result2))))


;;;;;

(def cluster (sut/new-cluster cluster-data))


(deftest new-node-object-test
  (testing "Create NodeObject instance is successful"
    (let [node-object (sut/new-node-object {:id   #uuid "00000000-0000-0000-0000-000000000001"
                                            :host "127.0.0.1"
                                            :port 5376}
                        cluster)]

      (is (instance? NodeObject node-object) "Should be NodeObject instance")
      (is (s/valid? ::spec/node (sut/get-value node-object)) "Node inside NodeObject has correct structure")

      (testing "Node object has correct structure"
        (m/assert ::spec/node (sut/get-value node-object))
        (m/assert ^:matcho/strict
          {:id                   #uuid "00000000-0000-0000-0000-000000000001"
           :host                 "127.0.0.1"
           :port                 5376
           :cluster              cluster
           :status               :stop
           :neighbours           {}
           :restart-counter      0
           :tx                   0
           :ping-events          {}
           :indirect-ping-events {}
           :payload              {}
           :scheduler-pool       #(instance? MutablePool %)
           :*udp-server          nil
           :outgoing-events      []
           :ping-round-buffer    []
           :probe-events         {}}
          (sut/get-value node-object)))))

  (testing "Wrong node data is prohibited"
    (is (thrown-with-msg? Exception #"Invalid node data"
          (sut/new-node-object {:a 1} cluster)))))


(deftest getters-test
  (testing "getters"
    (let [this (sut/new-node-object node-data1 cluster)]

      (m/assert ^:matcho/strict
        {:id                   #uuid "00000000-0000-0000-0000-000000000001"
         :host                 "127.0.0.1"
         :port                 5376
         :cluster              cluster
         :status               :stop
         :neighbours           {}
         :restart-counter      7
         :tx                   0
         :ping-events          {}
         :indirect-ping-events {}
         :payload              {}
         :scheduler-pool       #(instance? MutablePool %)
         :*udp-server          nil
         :outgoing-events      []
         :ping-round-buffer    []
         :probe-events         {}}
        (sut/get-value this))

      (m/assert #uuid "00000000-0000-0000-0000-000000000001" (sut/get-id this))
      (m/assert "127.0.0.1" (sut/get-host this))
      (m/assert 5376 (sut/get-port this))
      (m/assert 7 (sut/get-restart-counter this))
      (m/assert 0 (sut/get-tx this))
      (m/assert cluster (sut/get-cluster this))
      (m/assert 3 (sut/get-cluster-size this))
      (m/assert empty? (sut/get-payload this))

      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))

      (m/assert {#uuid "00000000-0000-0000-0000-000000000002"
                 (dissoc (sut/new-neighbour-node neighbour-data1) :updated-at)}
        (sut/get-neighbours this))

      (m/assert ^:matcho/strict
        {:id              #uuid "00000000-0000-0000-0000-000000000002"
         :host            "127.0.0.1"
         :port            5377
         :status          :alive
         :access          :direct
         :payload         {:tcp-port 4567}
         :restart-counter 3
         :tx              0
         :updated-at      pos-int?}
        (sut/get-neighbour this #uuid "00000000-0000-0000-0000-000000000002"))

      (m/assert :stop (sut/get-status this))
      (m/assert empty? (sut/get-outgoing-events this))

      (sut/upsert-ping this (event/empty-ping))
      (sut/upsert-indirect-ping this (event/empty-indirect-ping))

      (m/assert
        {#uuid"00000000-0000-0000-0000-000000000001"
         {:cmd-type        0
          :id              #uuid"00000000-0000-0000-0000-000000000000"
          :host            "localhost"
          :port            1
          :restart-counter 0
          :tx              0
          :neighbour-id    #uuid"00000000-0000-0000-0000-000000000001"
          :attempt-number  1}}
        (sut/get-ping-events this))

      (m/assert
        {#uuid "00000000-0000-0000-0000-000000000002"
         {:cmd-type          14
          :id                #uuid "00000000-0000-0000-0000-000000000000"
          :host              "localhost"
          :port              1
          :restart-counter   0
          :tx                0
          :intermediate-host "localhost"
          :intermediate-id   #uuid "00000000-0000-0000-0000-000000000001"
          :intermediate-port 10
          :neighbour-host    "localhost"
          :neighbour-id      #uuid "00000000-0000-0000-0000-000000000002"
          :neighbour-port    100
          :attempt-number    1}}
        (sut/get-indirect-ping-events this))

      (m/assert
        {:cmd-type        0
         :id              #uuid"00000000-0000-0000-0000-000000000000"
         :host            "localhost"
         :port            1
         :restart-counter 0
         :tx              0
         :neighbour-id    #uuid"00000000-0000-0000-0000-000000000001"
         :attempt-number  1}
        (sut/get-ping-event this #uuid"00000000-0000-0000-0000-000000000001"))

      (m/assert
        {:cmd-type        14
         :id              #uuid"00000000-0000-0000-0000-000000000000"
         :host            "localhost"
         :port            1
         :restart-counter 0
         :tx              0
         :neighbour-id    #uuid"00000000-0000-0000-0000-000000000002"
         :attempt-number  1}
        (sut/get-indirect-ping-event this #uuid "00000000-0000-0000-0000-000000000002"))

      (m/assert [] (sut/get-ping-round-buffer this))

      (sut/insert-probe this (event/empty-probe))

      (m/assert {#uuid"00000000-0000-0009-0000-000000000001" nil}
        (sut/get-probe-events this))

      (m/assert nil
        (sut/get-probe-event this #uuid"00000000-0000-0009-0000-000000000001"))

      (sut/delete-probe this #uuid"00000000-0000-0009-0000-000000000001")
      (m/assert nil (sut/get-probe-event this #uuid"00000000-0000-0009-0000-000000000001")))))


(deftest nodes-in-cluster-test
  (let [this (sut/new-node-object node-data1 cluster)
        nb   (sut/new-neighbour-node neighbour-data1)]
    (testing "In cluster only this node"
      (is (= 1 (sut/nodes-in-cluster this)) "Should be cluster with one node"))
    (testing "In cluster this node and one neighbour"
      (sut/upsert-neighbour this nb)
      (is (= 2 (sut/nodes-in-cluster this)) "Should be cluster with two nodes"))))


(deftest set-cluster-test
  (testing "set cluster"
    (let [new-cluster (sut/new-cluster (assoc cluster-data :id (random-uuid) :name "cluster2"))
          this        (sut/new-node-object node-data1 cluster)]
      (sut/set-cluster this new-cluster)
      (m/assert (sut/get-cluster this) new-cluster)
      (is (thrown-with-msg? Exception #"Invalid cluster data"
            (sut/set-cluster this (assoc cluster :id 1))))
      (testing "Cluster change allowed only in status :stop"
        (is (thrown-with-msg? Exception #"Node is not stopped"
              (sut/set-status this :left)
              (sut/set-cluster this new-cluster)))))))


(deftest set-cluster-size-test
  (testing "set cluster size"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert 3 (sut/get-cluster-size this))
      (sut/set-cluster-size this 5)
      (m/assert 5 (sut/get-cluster-size this))
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid cluster size"
              (sut/set-cluster-size this -1)))))))


(deftest cluster-size-exceed?-test
  (let [this (sut/new-node-object node-data1 (assoc cluster :cluster-size 1))]
    (is (true? (sut/cluster-size-exceed? this)))
    (sut/set-cluster-size this 3)
    (is (false? (sut/cluster-size-exceed? this)))))



(deftest set-status-test
  (testing "set status"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert :stop (sut/get-status this))
      (sut/set-status this :left)
      (m/assert :left (sut/get-status this))
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid node status"
              (sut/set-status this :wrong-value)))))))


(deftest set-payload-test
  (testing "set payload"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-payload this))
      (sut/set-payload this {:tcp-port 1234 :role "data node"})
      (m/assert {:tcp-port 1234 :role "data node"} (sut/get-payload this))
      (testing "too big payload cannot be set"
        (is (thrown-with-msg? Exception #"Size of payload is too big"
              (sut/set-payload this
                {:long-string (apply str (repeat (:max-payload-size @sut/*config) "a"))})))))))


(deftest set-restart-counter-test
  (testing "set restart counter"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert 7 (sut/get-restart-counter this))
      (sut/set-restart-counter this 42)
      (m/assert 42 (sut/get-restart-counter this))
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid restart counter data"
              (sut/set-restart-counter this -1)))))))


(deftest inc-tx-test
  (testing "increment tx"
    (let [new-cluster (sut/new-cluster (assoc cluster-data :id (random-uuid) :name "cluster2"))
          this        (sut/new-node-object node-data1 cluster)]
      (m/assert 0 (sut/get-tx this))
      (sut/inc-tx this)
      (m/assert 1 (sut/get-tx this)))))


(deftest upsert-neighbour-test
  (testing "upsert neighbour"
    (let [this (sut/new-node-object node-data1 (assoc cluster :cluster-size 99))]
      (m/assert empty? (sut/get-neighbours this))
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data2))
      (m/assert (dissoc neighbour-data1 :updated-at)
        (sut/get-neighbour this (:id neighbour-data1)))
      (m/assert 2 (count (sut/get-neighbours this)))

      (testing "don't put to neighbours with the same id as this node has"
        (m/assert 2 (count (sut/get-neighbours this)))
        (sut/upsert-neighbour this neighbour-data3)
        (m/assert 2 (count (sut/get-neighbours this))))

      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid neighbour node data"
              (sut/upsert-neighbour this {:a :bad-value}))))
      (testing "Update timestamp after every neighbour update"
        (let [nb        (sut/get-neighbour this (:id neighbour-data1))
              timestamp (:updated-at nb)
              port      (:port nb)]
          (m/assert pos-int? timestamp)
          (m/assert pos-int? port)
          (sut/upsert-neighbour this (sut/new-neighbour-node (assoc neighbour-data1 :port 5370)))
          (let [modified-nb (sut/get-neighbour this (:id neighbour-data1))]
            (is (not= timestamp (:updated-at modified-nb)))
            (is (not= port (:port modified-nb))))))))

  (testing "upsert neighbour should success if node exists in a neighbours table"
    (let [this (sut/new-node-object node-data1 (assoc cluster :cluster-size 2))]
      (m/assert empty? (sut/get-neighbours this))
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
      (m/assert 1 (count (sut/get-neighbours this)))

      (testing "do not upsert new neighbour if cluster size exceeded"
        (is (thrown-with-msg? Exception #"Cluster size exceeded"
              (sut/upsert-neighbour this neighbour-data3))))

      (testing "upsert existing neighbour should success"
        (sut/upsert-neighbour this (assoc neighbour-data1 :restart-counter 4))
        (m/assert {:restart-counter 4} (sut/get-neighbour this (:id neighbour-data1)))
        (m/assert 1 (count (sut/get-neighbours this)))))))


(deftest delete-neighbour-test
  (testing "delete neighbour"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-neighbours this))
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data2))
      (m/assert 2 (count (sut/get-neighbours this)))
      (sut/delete-neighbour this (:id neighbour-data1))
      (m/assert 1 (count (sut/get-neighbours this)))
      (m/assert (dissoc neighbour-data2 :updated-at)
        (sut/get-neighbour this (:id neighbour-data2))))))


(deftest set-outgoing-events-test
  (testing "set outgoing events"
    (let [new-outgoing-events [[(:left event/code) (random-uuid)]]
          this                (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-outgoing-events this))
      (sut/set-outgoing-events this new-outgoing-events)
      (m/assert ^:matcho/strict
        new-outgoing-events
        (sut/get-outgoing-events this)))))


(deftest put-event-test
  (testing "put event"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-outgoing-events this))
      (sut/put-event this (event/empty-ping))
      (sut/put-event this (event/empty-ack))
      (m/assert ^:matcho/strict
        [(event/empty-ping) (event/empty-ack)]
        (sut/get-outgoing-events this)))))



(deftest take-events-test
  (testing "take events"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-outgoing-events this))
      (sut/put-event this (event/empty-ping))
      (sut/put-event this (event/empty-ack))
      (m/assert 2 (count (sut/get-outgoing-events this)))
      (m/assert
        [(event/empty-ping) (event/empty-ack)]
        (sut/take-events this 2))
      (m/assert 0 (count (sut/get-outgoing-events this)))))
  (testing "take all events"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-outgoing-events this))
      (sut/put-event this (event/empty-ping))
      (sut/put-event this (event/empty-ack))
      (m/assert 2 (count (sut/get-outgoing-events this)))
      (m/assert
        [(event/empty-ping) (event/empty-ack)]
        (sut/take-events this))
      (m/assert 0 (count (sut/get-outgoing-events this))))))


(deftest upsert-ping-test
  (testing "upsert ping"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-ping-events this))

      (testing "Function returns the id of ping event"
        (m/assert #uuid "00000000-0000-0000-0000-000000000001"
          (sut/upsert-ping this (event/empty-ping))))

      (m/assert 1 (count (sut/get-ping-events this)))
      (m/assert
        {#uuid "00000000-0000-0000-0000-000000000001" (event/empty-ping)}
        (sut/get-ping-events this))

      (testing "Upsert ping event with same id will increment attempt number"
        (sut/upsert-ping this (event/empty-ping))
        (sut/upsert-ping this (event/empty-ping))
        (m/assert {:attempt-number 3}
          (sut/get-ping-event this #uuid"00000000-0000-0000-0000-000000000001")))

      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid ping event data"
              (sut/upsert-ping this {:a :bad-value})))))))


(deftest delete-ping-test
  (testing "delete ping"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-ping-events this))
      (sut/upsert-ping this (event/empty-ping))
      (m/assert 1 (count (sut/get-ping-events this)))
      (sut/delete-ping this #uuid "00000000-0000-0000-0000-000000000001")
      (m/assert zero? (count (sut/get-ping-events this))))))


(deftest upsert-indirect-ping-test
  (testing "upsert indirect ping"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-indirect-ping-events this))

      (testing "Function returns the id of indirect ping event"
        (m/assert #uuid "00000000-0000-0000-0000-000000000002"
          (sut/upsert-indirect-ping this (event/empty-indirect-ping))))

      (m/assert 1 (count (sut/get-indirect-ping-events this)))
      (m/assert {#uuid"00000000-0000-0000-0000-000000000002" (event/empty-indirect-ping)}
        (sut/get-indirect-ping-events this))

      (testing "Upsert indirect ping event with same id will increment attempt number"
        (sut/upsert-indirect-ping this (event/empty-indirect-ping))
        (sut/upsert-indirect-ping this (event/empty-indirect-ping))
        (m/assert {:attempt-number 3}
          (sut/get-indirect-ping-event this #uuid"00000000-0000-0000-0000-000000000002")))

      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid indirect ping event data"
              (sut/upsert-indirect-ping this {:a :bad-value})))))))


(deftest delete-indirect-ping-test
  (testing "delete indirect ping"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-indirect-ping-events this))
      (sut/upsert-indirect-ping this (event/empty-indirect-ping))
      (m/assert 1 (count (sut/get-indirect-ping-events this)))
      (sut/delete-indirect-ping this #uuid"00000000-0000-0000-0000-000000000002")
      (m/assert zero? (count (sut/get-indirect-ping-events this))))))


(deftest upsert-probe-test
  (testing "upsert probe"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-probe-events this))
      (sut/insert-probe this (event/empty-probe))
      (m/assert 1 (count (sut/get-probe-events this)))
      (m/assert
        {#uuid "00000000-0000-0009-0000-000000000001" nil}
        (sut/get-probe-events this))
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid probe event data"
              (sut/insert-probe this {:a :bad-value})))))))


(deftest delete-probe-test
  (testing "delete probe"
    (let [this (sut/new-node-object node-data1 cluster)]
      (m/assert empty? (sut/get-probe-events this))
      (sut/insert-probe this (event/empty-probe))
      (m/assert 1 (count (sut/get-probe-events this)))
      (sut/delete-probe this #uuid "00000000-0000-0009-0000-000000000001")
      (m/assert zero? (count (sut/get-probe-events this))))))


(deftest upsert-probe-ack-test
  (testing "upsert ack probe"
    (let [this            (sut/new-node-object node-data1 cluster)
          probe-event     (event/empty-probe)
          probe-ack-event (sut/new-probe-ack-event this probe-event)
          probe-key       (.-probe_key probe-event)]
      (m/assert empty? (sut/get-probe-events this))
      (sut/insert-probe this (event/empty-probe))
      (m/assert 1 (count (sut/get-probe-events this)))
      (sut/upsert-probe-ack this probe-ack-event)
      (m/assert
        {probe-key
         {:cmd-type        10
          :id              #uuid "00000000-0000-0000-0000-000000000001"
          :host            "127.0.0.1"
          :port            5376
          :restart-counter 7
          :tx              0
          :status          :stop
          :neighbour-id    #uuid "00000000-0000-0000-0000-000000000000"
          :neighbour-tx    0
          :probe-key       #uuid "00000000-0000-0009-0000-000000000001"}}
        (sut/get-probe-events this))
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid probe ack event data"
              (sut/upsert-probe-ack this {:a :bad-value})))))))


;;;;;;;;;;;;;;;;;

;;;;
;; Event builders tests
;;;;


(deftest new-probe-event-test
  (let [this        (sut/new-node-object node-data1 cluster)
        probe-event (sut/new-probe-event this "127.0.0.1" 5376)]
    (m/assert ProbeEvent (type probe-event))
    (m/assert ::spec/probe-event probe-event)
    (m/assert uuid? (.-probe_key probe-event))
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid probe event"
            (sut/new-probe-event this "127.0.01" -1))))))


;;;;

(deftest new-probe-ack-event-test
  (let [this            (sut/new-node-object node-data1 cluster)
        probe-event     (sut/new-probe-event this "127.0.0.1" 5376)
        probe-ack-event (sut/new-probe-ack-event this probe-event)]
    (m/assert ProbeAckEvent (type probe-ack-event))
    (m/assert ::spec/probe-ack-event probe-ack-event)
    (m/assert (.-probe_key probe-event) (.-probe_key probe-ack-event))
    (testing "tx contains number of generated events on this node"
      (m/assert 2 (sut/get-tx this)))
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid probe ack event"
            (sut/new-probe-ack-event this (assoc probe-event :id :bad-value)))))))


;;;;

(deftest new-ping-event-test
  (let [this       (sut/new-node-object node-data1 cluster)
        ping-event (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)]
    (m/assert PingEvent (type ping-event))
    (m/assert ::spec/ping-event ping-event)
    (testing "tx contains number of generated events on this node"
      (m/assert 1 (sut/get-tx this)))
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid ping event"
            (sut/new-ping-event this :bad-value 42))))))


;;;;

(deftest new-ack-event-test
  (let [this       (sut/new-node-object node-data1 cluster)
        ping-event (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)
        ack-event  (sut/new-ack-event this ping-event)]
    (m/assert AckEvent (type ack-event))
    (m/assert ::spec/ack-event ack-event)
    (testing "tx contains number of generated events on this node"
      (m/assert 2 (sut/get-tx this)))
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid ack event"
            (sut/new-ack-event this (assoc ping-event :id :bad-value)))))))


;;;;

(deftest new-indirect-ping-event-test

  (let [this         (sut/new-node-object node-data1 cluster)
        intermediate (sut/new-neighbour-node neighbour-data1)
        neighbour    (sut/new-neighbour-node neighbour-data2)]

    (sut/upsert-neighbour this intermediate)
    (sut/upsert-neighbour this neighbour)

    (testing "Create normal indirect ping event"

      (let [indirect-ping-event (sut/new-indirect-ping-event this (:id intermediate) (:id neighbour) 1)]
        (m/assert IndirectPingEvent (type indirect-ping-event))
        (m/assert ::spec/indirect-ping-event indirect-ping-event)
        (m/assert {:intermediate-id   (:id intermediate)
                   :intermediate-host (:host intermediate)
                   :intermediate-port (:port intermediate)}
          indirect-ping-event)

        (testing "tx contains number of generated events on this node"
          (m/assert 1 (sut/get-tx this)))

        (m/assert {:neighbour-id   (:id neighbour)
                   :neighbour-host (:host neighbour)
                   :neighbour-port (:port neighbour)}
          indirect-ping-event)))

    (testing "Unknown intermediate node is prohibited"
      (is (thrown-with-msg? Exception #"Unknown intermediate node with such id"
            (sut/new-indirect-ping-event this :bad-id (:id neighbour) 1))))

    (testing "Unknown neighbour node is prohibited"
      (is (thrown-with-msg? Exception #"Unknown neighbour node with such id"
            (sut/new-indirect-ping-event this (:id intermediate) :bad-id 1))))

    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid indirect ping event"
            (sut/new-indirect-ping-event this (:id intermediate) (:id neighbour) :bad-value))))))


;;;;

(deftest new-indirect-ack-event-test

  (let [this                (sut/new-node-object node-data1 cluster)
        intermediate        (sut/new-neighbour-node neighbour-data1)
        neighbour           (sut/new-neighbour-node neighbour-data2)
        _                   (sut/upsert-neighbour this intermediate)
        _                   (sut/upsert-neighbour this neighbour)
        indirect-ping-event (sut/new-indirect-ping-event this (:id intermediate) (:id neighbour) 1)
        neighbour-this      (sut/new-node-object node-data3 cluster)]

    (testing "tx contains number of generated events on this node"
      (m/assert 1 (sut/get-tx this)))

    (testing "Create normal indirect ack event"

      (let [indirect-ack-event (sut/new-indirect-ack-event neighbour-this indirect-ping-event)]
        (m/assert IndirectAckEvent (type indirect-ack-event))
        (m/assert ::spec/indirect-ack-event indirect-ack-event)
        (m/assert {:intermediate-id   (:id intermediate)
                   :intermediate-host (:host intermediate)
                   :intermediate-port (:port intermediate)}
          indirect-ack-event)
        (m/assert {:neighbour-id   (sut/get-id this)
                   :neighbour-host (sut/get-host this)
                   :neighbour-port (sut/get-port this)}
          indirect-ack-event)))

    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid indirect ack event"
            (sut/new-indirect-ack-event this (assoc indirect-ping-event :attempt-number -1)))))))


;;;;


(deftest new-alive-event-test
  (let [this           (sut/new-node-object node-data1 cluster)
        neighbour-this (sut/new-node-object node-data2 cluster)
        ping-event     (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)
        ack-event      (sut/new-ack-event neighbour-this ping-event)
        alive-event    (sut/new-alive-event this ack-event)]
    (testing "tx contains number of generated events on this node"
      (m/assert 2 (sut/get-tx this)))
    (m/assert AliveEvent (type alive-event))
    (m/assert ::spec/alive-event alive-event)
    (testing "Alive event contains tx less by 1 than node has"
      (m/assert {:neighbour-id #uuid "00000000-0000-0000-0000-000000000002"
                 :neighbour-tx (dec (sut/get-tx neighbour-this))} alive-event))
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid alive event"
            (sut/new-alive-event this (assoc ack-event :id :bad-value)))))))


;;;;

(deftest new-cluster-size-event-test
  (let [this             (sut/new-node-object node-data1 cluster)
        new-cluster-size 5
        ncs-event        (sut/new-cluster-size-event this new-cluster-size)]
    (testing "tx contains number of generated events on this node"
      (m/assert 1 (sut/get-tx this)))
    (m/assert NewClusterSizeEvent (type ncs-event))
    (m/assert ::spec/new-cluster-size-event ncs-event)
    (m/assert {:old-cluster-size (.-cluster_size cluster)
               :new-cluster-size new-cluster-size}
      ncs-event)
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid new cluster size event"
            (sut/new-cluster-size-event this -1))))))



;;;;

(deftest new-dead-event-test
  (let [this       (sut/new-node-object node-data1 cluster)
        ping-event (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)
        neighbour  (sut/new-neighbour-node neighbour-data1)
        _          (sut/upsert-neighbour this neighbour)
        dead-event (sut/new-dead-event this (.-neighbour_id ping-event))]
    (testing "tx contains number of generated events on this node"
      (m/assert 2 (sut/get-tx this)))
    (m/assert DeadEvent (type dead-event))
    (m/assert ::spec/dead-event dead-event)
    (m/assert {:neighbour-id #uuid "00000000-0000-0000-0000-000000000002"}
      dead-event)
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid dead event"
            (sut/new-dead-event this (assoc ping-event :id :bad-value)))))))


;;;;

(deftest new-anti-entropy-event-test
  (let [this               (sut/new-node-object node-data1 cluster)
        neighbour          (sut/new-neighbour-node neighbour-data1)
        _                  (sut/upsert-neighbour this neighbour)
        anti-entropy-event (sut/new-anti-entropy-event this)]
    (testing "tx contains number of generated events on this node"
      (m/assert 1 (sut/get-tx this)))
    (m/assert AntiEntropy (type anti-entropy-event))
    (m/assert ::spec/anti-entropy-event anti-entropy-event)
    (m/assert {:anti-entropy-data [[#uuid "00000000-0000-0000-0000-000000000002"
                                    "127.0.0.1"
                                    5377
                                    3
                                    0
                                    3
                                    0
                                    {:tcp-port 4567}]]}
      anti-entropy-event)))


;;;;

(deftest new-join-event-test
  (let [this       (sut/new-node-object node-data1 cluster)
        join-event (sut/new-join-event this)]
    (testing "tx contains number of generated events on this node"
      (m/assert 1 (sut/get-tx this)))
    (m/assert JoinEvent (type join-event))
    (m/assert ::spec/join-event join-event)
    (m/assert {:cmd-type        (:join event/code)
               :id              (sut/get-id this)
               :restart-counter (sut/get-restart-counter this)
               :tx              (dec (sut/get-tx this))}
      join-event)))


;;;;

(deftest new-suspect-event-test
  (let [this          (sut/new-node-object node-data1 cluster)
        neighbour     (sut/new-neighbour-node neighbour-data1)
        _             (sut/upsert-neighbour this neighbour)
        suspect-event (sut/new-suspect-event this (:id neighbour))]
    (testing "tx contains number of generated events on this node"
      (m/assert 1 (sut/get-tx this)))
    (m/assert SuspectEvent (type suspect-event))
    (m/assert ::spec/suspect-event suspect-event)
    (m/assert {:neighbour-id #uuid "00000000-0000-0000-0000-000000000002"}
      suspect-event)
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid suspect event data"
            (sut/new-suspect-event this :bad-value))))))


;;;;

(deftest new-left-event-test
  (let [this       (sut/new-node-object node-data1 cluster)
        left-event (sut/new-left-event this)]
    (testing "tx contains number of generated events on this node"
      (m/assert 1 (sut/get-tx this)))
    (m/assert LeftEvent (type left-event))
    (m/assert ::spec/left-event left-event)
    (m/assert {:cmd-type        (:left event/code)
               :id              (sut/get-id this)
               :restart-counter (sut/get-restart-counter this)
               :tx              (dec (sut/get-tx this))}
      left-event)))


;;;;

(deftest new-payload-event-test
  (let [this          (sut/new-node-object node-data1 cluster)
        new-payload   {:a 1 :b [3]}
        _             (sut/set-payload this new-payload)
        payload-event (sut/new-payload-event this)]
    (testing "tx contains number of generated events on this node"
      (m/assert 1 (sut/get-tx this)))
    (m/assert PayloadEvent (type payload-event))
    (m/assert ::spec/payload-event payload-event)
    (m/assert {:cmd-type        (:payload event/code)
               :id              (sut/get-id this)
               :restart-counter (sut/get-restart-counter this)
               :tx              (dec (sut/get-tx this))
               :payload         (sut/get-payload this)}
      payload-event)))



;;;;;;;;;;;;;;;;;;
;;; Utility functions tests


(deftest suitable-restart-counter?-test
  (let [node1 (sut/new-node-object node-data1 cluster)
        node2 (sut/new-node-object node-data2 cluster)
        ping1 (sut/new-ping-event node1 (sut/get-id node2) 42)]

    (testing "Accept restart counter from normal neighbour"
      ;; add node1(neighbour-data3) as new normal neighbour in map
      (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))
      (is (true? (sut/suitable-restart-counter? node2 ping1)))

      (testing "Deny older restart counter from neighbour"
        ;; upsert neighbour with restart counter bigger than ping has
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (update neighbour-data3 :restart-counter inc)))
        (is (not (sut/suitable-restart-counter? node2 ping1))))

      (testing "Nil or absent value for neighbour id will not crash"
        (is (not (sut/suitable-restart-counter? node2 nil)))
        (is (not (sut/suitable-restart-counter? node2 {:id 123})))))))


(deftest suitable-tx?-test
  (let [node1 (sut/new-node-object node-data1 cluster)
        node2 (sut/new-node-object node-data2 cluster)
        _     (sut/inc-tx node1)
        ping1 (sut/new-ping-event node1 (sut/get-id node2) 42)]

    (testing "Accept tx from normal neighbour"
      ;; add new normal neighbour in map
      (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))
      (is (true? (sut/suitable-tx? node2 ping1))))

    (testing "Deny older tx from neighbour"
      ;; upsert neighbour with tx bigger than ping has
      (sut/upsert-neighbour node2 (sut/new-neighbour-node (update neighbour-data3 :tx inc)))
      (is (not (sut/suitable-tx? node2 ping1))))

    (testing "Nil or absent value for neighbour id will not crash"
      (is (not (sut/suitable-tx? node2 nil)))
      (is (not (sut/suitable-tx? node2 {:id 123}))))))


(deftest suitable-incarnation?-test
  (let [node1 (sut/new-node-object node-data1 cluster)
        node2 (sut/new-node-object node-data2 cluster)
        _     (sut/inc-tx node1)
        ping1 (sut/new-ping-event node1 (sut/get-id node2) 42)]

    (testing "ping from normal neighbour should be accepted"
      ;; add new normal neighbour in map
      (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))
      (is (true? (sut/suitable-incarnation? node2 ping1))))


    (testing "ping with older tx should be denied"
      ;; add neighbour with tx bigger than ping has
      (sut/upsert-neighbour node2 (sut/new-neighbour-node (update neighbour-data3 :tx inc)))
      (is (not (sut/suitable-incarnation? node2 ping1))))

    (testing "ping with older restart counter should be denied"
      ;; add neighbour with tx bigger than ping has
      (sut/upsert-neighbour node2 (sut/new-neighbour-node (update neighbour-data3 :restart-counter inc)))
      (is (not (sut/suitable-incarnation? node2 ping1))))

    (testing "Nil or absent value for neighbour id will not crash"
      (is (not (sut/suitable-incarnation? node2 nil)))
      (is (not (sut/suitable-incarnation? node2 {:id 123}))))))


;;;;


(deftest neighbour->vec-test
  (let [nb (sut/new-neighbour-node neighbour-data1)]
    (m/assert [#uuid "00000000-0000-0000-0000-000000000002"
               "127.0.0.1"
               5377                                         ;; port
               3                                            ;; :alive = 3
               0                                            ;; 0 - direct, 1 - indirect
               3                                            ;; restart-counter
               0                                            ;; tx
               {:tcp-port 4567}]
      (sut/neighbour->vec nb))))


(deftest vec->neighbour-test
  (testing "Converting NeighbourNode to vector and restore from vector to NeighbourNode is successful"
    (let [nb          (sut/new-neighbour-node neighbour-data1)
          nbv         (sut/neighbour->vec nb)
          restored-nb (sut/vec->neighbour nbv)]
      (m/assert (dissoc nb :updated-at) (dissoc restored-nb :updated-at)))))


(deftest build-anti-entropy-data-test
  (let [node1 (sut/new-node-object node-data1 cluster)]

    (testing "Anti entropy is build successfully"
      ;; add new normal neighbour in map
      (sut/upsert-neighbour node1 (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000002"
                                                           :host            "127.0.0.1"
                                                           :port            5432
                                                           :status          :alive
                                                           :access          :direct
                                                           :restart-counter 2
                                                           :tx              2
                                                           :payload         {}
                                                           :updated-at      (System/currentTimeMillis)}))
      (sut/upsert-neighbour node1 (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000003"
                                                           :host            "127.0.0.1"
                                                           :port            5433
                                                           :status          :alive
                                                           :access          :direct
                                                           :restart-counter 3
                                                           :tx              3
                                                           :payload         {}
                                                           :updated-at      (System/currentTimeMillis)}))

      (is (= 2 (count (sut/build-anti-entropy-data node1))) "Default size of vector as expected")
      (is (= 1 (count (sut/build-anti-entropy-data node1 :num 1))) "Size of vector as requested")
      (is (= [] (sut/build-anti-entropy-data (sut/new-node-object node-data2 cluster)))
        "Empty neighbours map should produce empty anti-entropy vector")
      (m/assert [[#uuid "00000000-0000-0000-0000-000000000002"
                  "127.0.0.1"
                  5432
                  3
                  0
                  2
                  2
                  {}]
                 [#uuid "00000000-0000-0000-0000-000000000003"
                  "127.0.0.1"
                  5433
                  3
                  0
                  3
                  3
                  {}]]
        (sort (sut/build-anti-entropy-data node1))))))


;;;;;;;;;;;;;;;;;
;; Send event tests


(defn empty-node-process-fn
  "Run empty node process"
  [^NodeObject this]
  (while (-> this sut/get-value :*udp-server deref :continue?)
    (Thread/sleep 5)))


(defn set-payload-incoming-data-processor-fn
  "Set received events to payload section without processing them."
  [^NodeObject this ^bytes encrypted-data]
  (let [secret-key     (-> this sut/get-cluster :secret-key)
        decrypted-data (sut/safe (e/decrypt-data ^bytes secret-key ^bytes encrypted-data))
        events-vector  (sut/deserialize ^bytes decrypted-data)]
    (sut/set-payload this events-vector)))


(deftest send-event-test
  (testing "node1 can send event to node2"
    (let [big-cluster (assoc cluster :cluster-size 999)
          node1       (sut/new-node-object node-data1 big-cluster)
          node2       (sut/new-node-object node-data2 big-cluster)
          probe-event (sut/new-probe-event node1 (sut/get-host node2) (sut/get-port node2))]
      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start node1 empty-node-process-fn #(do [%1 %2]))

        (testing "send event using host and port"
          (let [*expecting-event (promise)]

            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))

            (sut/send-event node1 probe-event (sut/get-host node2) (sut/get-port node2))

            (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
            (m/assert [(.prepare probe-event)] (sut/get-payload node2))

            (remove-watch (:*node node2) :event-detect)))

        (testing "send event using neighbour id"
          (let [*expecting-event (promise)]
            (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))

            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))

            (sut/send-event node1 (event/empty-ack) (sut/get-id node2))

            (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
            (m/assert [(.prepare (event/empty-ack))] (sut/get-payload node2))

            (remove-watch (:*node node2) :event-detect)))

        (testing "Wrong neighbour id is prohibited"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/send-event node1 (event/empty-ack) (random-uuid)))))

        (testing "Too big UDP packet is prohibited"
          (let [config @sut/*config]
            (swap! sut/*config assoc :max-anti-entropy-items 100)
            (dotimes [n 100]                                ;; fill too many neighbours
              (sut/upsert-neighbour node1 (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))

            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (sut/send-event node1 (sut/new-anti-entropy-event node1) (sut/get-id node2))))

            (reset! sut/*config config)))


        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop node1)
          (sut/stop node2))))))


;;;;

(deftest send-event-ae-test
  (testing "node1 can send event to node2"
    (let [big-cluster (assoc cluster :cluster-size 999)
          node1       (sut/new-node-object node-data1 big-cluster)
          node2       (sut/new-node-object node-data2 big-cluster)
          probe-event (sut/new-probe-event node1 (sut/get-host node2) (sut/get-port node2))]
      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start node1 empty-node-process-fn #(do [%1 %2]))

        (testing "send event using host and port"
          (let [*expecting-event (promise)]

            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))

            (sut/send-event-ae node1 probe-event (sut/get-host node2) (sut/get-port node2))

            (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
            (m/assert [(.prepare probe-event) (.prepare (update (sut/new-anti-entropy-event node1) :tx dec))]
              (sut/get-payload node2))

            (remove-watch (:*node node2) :event-detect)))

        (testing "send event using neighbour id"
          (let [*expecting-event (promise)]
            (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))

            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))

            (sut/send-event-ae node1 (event/empty-ack) (sut/get-id node2))

            (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
            (m/assert
              [(.prepare (event/empty-ack)) (.prepare (update (sut/new-anti-entropy-event node1) :tx dec))]
              (sut/get-payload node2))

            (remove-watch (:*node node2) :event-detect)))

        (testing "Wrong neighbour id is prohibited"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/send-event-ae node1 (event/empty-ack) (random-uuid)))))

        (testing "Too big UDP packet is prohibited"
          (let [config @sut/*config]
            (swap! sut/*config assoc :max-anti-entropy-items 100)
            (dotimes [n 100]                                ;; fill too many neighbours
              (sut/upsert-neighbour node1 (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))

            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (sut/send-event-ae node1 (sut/new-anti-entropy-event node1) (sut/get-id node2))))

            (reset! sut/*config config)))

        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop node1)
          (sut/stop node2))))))


(deftest send-events-test
  (testing "node1 can send all events to node2 with anti-entropy data"
    (let [node1       (sut/new-node-object node-data1 (assoc cluster :cluster-size 999))
          node2       (sut/new-node-object node-data2 (assoc cluster :cluster-size 999))
          probe-event (sut/new-probe-event node1 (sut/get-host node2) (sut/get-port node2))]

      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start node1 empty-node-process-fn #(do [%1 %2]))

        (testing "send all events using host and port"
          (let [*expecting-event (promise)]
            (sut/put-event node1 probe-event)
            (sut/put-event node1 (event/empty-ack))
            (sut/put-event node1 (sut/new-anti-entropy-event node1))

            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))

            (sut/send-events node1 (sut/take-events node1) (sut/get-host node2) (sut/get-port node2))

            (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
            (m/assert
              [(.prepare probe-event) (.prepare (event/empty-ack)) (.prepare (update (sut/new-anti-entropy-event node1) :tx dec))]
              (sut/get-payload node2))

            (testing "After send outgoing queue should be empty"
              (m/assert empty? (sut/get-outgoing-events node1)))

            (remove-watch (:*node node2) :event-detect)))

        (testing "Too big UDP packet is prohibited"
          (let [config @sut/*config]                        ;; increase from 2 to 100
            (swap! sut/*config assoc :max-anti-entropy-items 100)
            (dotimes [n 100]                                ;; fill too many neighbours
              (sut/upsert-neighbour node1 (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))

            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (sut/send-events node1
                    [(sut/new-anti-entropy-event node1)] (sut/get-host node2) (sut/get-port node2))))

            (reset! sut/*config config)))

        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop node1)
          (sut/stop node2))))))


(deftest get-neighbours-with-status-test
  (let [this (sut/new-node-object node-data1 (assoc cluster :cluster-size 99))
        nb0  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :left))
        nb1  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :stop))
        nb2  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :alive))
        nb3  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :alive))
        nb4  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :dead))
        nb5  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :dead))
        nb6  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :dead))
        nb7  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :suspect))
        nb8  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :suspect))
        nb9  (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :suspect))
        nb10 (sut/new-neighbour-node (assoc neighbour-data2 :id (random-uuid) :status :suspect))]
    (sut/upsert-neighbour this nb0)
    (sut/upsert-neighbour this nb1)
    (sut/upsert-neighbour this nb2)
    (sut/upsert-neighbour this nb3)
    (sut/upsert-neighbour this nb4)
    (sut/upsert-neighbour this nb5)
    (sut/upsert-neighbour this nb6)
    (sut/upsert-neighbour this nb7)
    (sut/upsert-neighbour this nb8)
    (sut/upsert-neighbour this nb9)
    (sut/upsert-neighbour this nb10)

    (testing "Get all alive and left neighbours"
      (let [result (sut/get-neighbours-with-status this #{:alive :left})]
        (m/assert 3 (count result))))

    (testing "Get all alive neighbours"
      (let [result (sut/get-alive-neighbours this)]
        (m/assert 2 (count result))))

    (testing "Get all stopped neighbours"
      (let [result (sut/get-stopped-neighbours this)]
        (m/assert 1 (count result))))

    (testing "Get all dead neighbours"
      (let [result (sut/get-dead-neighbours this)]
        (m/assert 3 (count result))))

    (testing "Get all left neighbours"
      (let [result (sut/get-left-neighbours this)]
        (m/assert 1 (count result))))

    (testing "Get all suspect neighbours"
      (let [result (sut/get-suspect-neighbours this)]
        (m/assert 4 (count result))))))


(deftest get-oldest-neighbour-test
  (let [this (sut/new-node-object node-data1 (assoc cluster :cluster-size 99))
        nb0  (sut/new-neighbour-node (assoc neighbour-data2 :id #uuid"00000000-0000-0000-0000-000000000000"))
        nb11 (sut/new-neighbour-node (assoc neighbour-data2 :id #uuid"00000000-0000-0000-0000-000000000011"))
        nb2  (sut/new-neighbour-node (assoc neighbour-data2 :id #uuid"00000000-0000-0000-0000-000000000002"))
        nb3  (sut/new-neighbour-node (assoc neighbour-data2 :status :left :id #uuid"00000000-0000-0000-0000-000000000003"))]
    (sut/upsert-neighbour this nb0)
    (Thread/sleep 1)
    (sut/upsert-neighbour this nb11)
    (Thread/sleep 1)
    (sut/upsert-neighbour this nb2)
    (Thread/sleep 1)
    (sut/upsert-neighbour this nb3)
    (m/assert (dissoc (sut/get-oldest-neighbour this) :updated-at) (dissoc nb0 :updated-at))
    (sut/delete-neighbour this (:id nb0))
    (m/assert (dissoc (sut/get-oldest-neighbour this) :updated-at) (dissoc nb11 :updated-at))
    (m/assert (dissoc (sut/get-oldest-neighbour this #{:left}) :updated-at) (dissoc nb3 :updated-at))))


(deftest alive-neighbour?-test
  (testing "Result for neighbours with alive statuses should be true"
    (let [nb1 (sut/new-neighbour-node neighbour-data1)
          nb2 (sut/new-neighbour-node (assoc neighbour-data2 :status :suspect))]
      (m/assert true (sut/alive-neighbour? nb1))
      (m/assert true (sut/alive-neighbour? nb2))))

  (testing "Result for not alive neighbours should be false"
    (let [nb1 (sut/new-neighbour-node (assoc neighbour-data1 :status :left))
          nb2 (sut/new-neighbour-node (assoc neighbour-data2 :status :dead))
          nb3 (sut/new-neighbour-node (assoc neighbour-data3 :status :stop))]
      (m/assert false (sut/alive-neighbour? nb1))
      (m/assert false (sut/alive-neighbour? nb2))
      (m/assert false (sut/alive-neighbour? nb3)))))


(deftest alive-node?-test
  (testing "Result for nodes with alive statuses should be true"
    (let [node1 (sut/new-node-object node-data1 (assoc cluster :cluster-size 99))
          node2 (sut/new-node-object node-data2 (assoc cluster :cluster-size 99))]
      (sut/set-status node1 :alive)
      (sut/set-status node2 :suspect)
      (m/assert true (sut/alive-node? node1))
      (m/assert true (sut/alive-node? node2))))

  (testing "Result for not alive nodes should be false"
    (let [node1 (sut/new-node-object node-data1 (assoc cluster :cluster-size 99))
          node2 (sut/new-node-object node-data2 (assoc cluster :cluster-size 99))
          node3 (sut/new-node-object node-data3 (assoc cluster :cluster-size 99))]
      (sut/set-status node1 :dead)
      (sut/set-status node2 :left)
      (sut/set-status node2 :stop)
      (m/assert false (sut/alive-node? node1))
      (m/assert false (sut/alive-node? node2))
      (m/assert false (sut/alive-node? node3)))))


;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;
;; SWIM business logic tests
;;;;


(deftest probe-test
  (let [node1 (sut/new-node-object node-data1 (assoc cluster :cluster-size 1))
        node2 (sut/new-node-object node-data2 (assoc cluster :cluster-size 1))]
    (try

      (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
      (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

      (let [probe-key (sut/probe node1 (sut/get-host node2) (sut/get-port node2))]
        (testing "Probe should return probe key as UUID"
          (m/assert UUID (type probe-key)))
        (testing "Probe should insert probe key to the `probe-events` buffer"
          (is (contains? (sut/get-probe-events node1) probe-key))))

      (catch Exception e
        (println (ex-message e)))
      (finally
        (sut/stop node1)
        (sut/stop node2)))))


(deftest ^:logic probe-probe-ack-test

  (testing "Probe -> ProbeAck logic"

    (testing "Neighbour from Probe Ack should not added to neighbours map cause cluster size limit reached"
      (let [node1            (sut/new-node-object node-data1 (assoc cluster :cluster-size 1))
            node2            (sut/new-node-object node-data2 (assoc cluster :cluster-size 1))

            *expecting-event (promise)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :upsert-neighbour-cluster-size-exceeded-error)
                                   (deliver *expecting-error cmd))))]

        (try

          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)


          (add-watch (:*node node1) :event-detect
            (fn [_ _ _ new-val]
              (let [[_ probe-ack-event] (first (:probe-events new-val))]
                (when (not (nil? probe-ack-event))          ;; wait for probe-ack event from node2
                  (deliver *expecting-event (:probe-events new-val))))))

          (add-tap error-catcher-fn)

          (sut/probe node1 (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/dessert :timeout (deref *expecting-error *max-test-timeout* :timeout))
          (m/assert 1 (sut/nodes-in-cluster node1))
          (m/assert :upsert-neighbour-cluster-size-exceeded-error @*expecting-error)

          (remove-watch (:*node node1) :event-detect)
          (remove-tap error-catcher-fn)

          (catch Exception e
            (println (ex-message e)))
          (finally
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Probe Ack should not accepted cause probe request never send"
      (let [node1            (sut/new-node-object node-data1 (assoc cluster :cluster-size 1))
            node2            (sut/new-node-object node-data2 (assoc cluster :cluster-size 1))
            probe-event      (sut/new-probe-event node1 (:host node-data2) (:port node-data2))
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :probe-ack-event-probe-never-send-error)
                                   (deliver *expecting-error cmd))))]
        (try

          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

          (add-tap error-catcher-fn)

          (sut/send-event node2 (sut/new-probe-ack-event node2 probe-event) (:host node-data1) (:port node-data1))

          (m/dessert :timeout (deref *expecting-error *max-test-timeout* :timeout))
          (m/assert :probe-ack-event-probe-never-send-error @*expecting-error)

          (remove-tap error-catcher-fn)

          (catch Exception e
            (println (ex-message e)))
          (finally
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Normal Probe Ack"
      (let [node1            (sut/new-node-object node-data1 (assoc cluster :cluster-size 1))
            node2            (sut/new-node-object node-data2 (assoc cluster :cluster-size 1))
            before-tx1       (sut/get-tx node1)
            before-tx2       (sut/get-tx node2)
            *expecting-event (promise)]
        (try

          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

          (sut/set-cluster-size node1 3)                    ;; increase cluster size

          (add-watch (:*node node1) :event-detect
            (fn [_ _ _ new-val]
              (when-not (empty? (:neighbours new-val))
                (deliver *expecting-event 1))))

          (sut/probe node1 (sut/get-host node2) (sut/get-port node2))

          ;; wait for ack
          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))

          (testing "Neighbour from Probe Ack should be added to neighbours map cause cluster size limit is not reached"
            (m/assert 2 (sut/nodes-in-cluster node1))
            (m/assert (:id node-data2) (:id (sut/get-neighbour node1 (sut/get-id node2)))))

          (testing "tx on node 1 is incremented correctly"
            (m/assert (+ 3 before-tx1) (sut/get-tx node1))) ;; 1- start node, 2 - send probe, 3 - receive ack-probe
          (testing "tx on node 2 is incremented correctly"  ;; 1- start node, 2 -receive probe, 3 - send ack-probe
            (m/assert (+ 3 before-tx2) (sut/get-tx node2)))

          (remove-watch (:*node node1) :event-detect)

          (catch Exception e
            (println (ex-message e)))
          (finally
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Do not add neighbour to neighbours map if our status is :alive or :suspect"
      (let [node1            (sut/new-node-object node-data1 (assoc cluster :cluster-size 3))
            node2            (sut/new-node-object node-data2 (assoc cluster :cluster-size 3))
            *expecting-event (promise)
            event-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :probe-ack-event)
                                   (deliver *expecting-event cmd))))]
        (try
          (add-tap event-catcher-fn)
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/set-cluster-size node1 3)                    ;; increase cluster size

          ;; check we have only this node in cluster
          (m/assert 1 (sut/nodes-in-cluster node1))
          ;; change status to any :alive or :suspect to verify that neighbour will be not added
          (sut/set-status node1 :alive)
          (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
          (sut/set-status node1 :suspect)
          (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/assert 1 (sut/nodes-in-cluster node1))
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap event-catcher-fn)
            (sut/stop node1)
            (sut/stop node2)))))))


(deftest ^:logic anti-entropy-test

  (testing "Accept normal anti-entropy event from node1 on node2"
    (let [node1 (sut/new-node-object node-data1 cluster)
          node2 (sut/new-node-object node-data2 cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))

        (let [ae-event         (sut/new-anti-entropy-event node1)
              *expecting-event (promise)
              event-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :anti-entropy-event)
                                     (deliver *expecting-event cmd))))]

          (add-tap event-catcher-fn)

          (m/assert 1 (count (sut/get-neighbours node2)))

          ;; sending normal anti-entropy event to node 2
          (sut/send-event node1 ae-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/assert 2 (count (sut/get-neighbours node2)))
          (m/assert (dissoc neighbour-data2 :updated-at) (sut/get-neighbour node2 (:id neighbour-data2)))

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do nothing if event from unknown node"
    (let [node1 (sut/new-node-object node-data1 cluster)
          node2 (sut/new-node-object node-data2 cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))

        (let [ae-event         (sut/new-anti-entropy-event node1)
              *expecting-event (promise)
              error-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :anti-entropy-event-unknown-neighbour-error)
                                     (deliver *expecting-event cmd))))]

          (add-tap error-catcher-fn)

          (m/assert 0 (count (sut/get-neighbours node2)))

          ;; sending normal anti-entropy event to node 2
          (sut/send-event node1 ae-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/assert :anti-entropy-event-unknown-neighbour-error @*expecting-event)
          (m/assert 0 (count (sut/get-neighbours node2)))

          (remove-tap error-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do nothing if event contains bad restart counter"
    (let [node1 (sut/new-node-object node-data1 cluster)
          node2 (sut/new-node-object node-data2 cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node
                                      (assoc neighbour-data3 :restart-counter 999)))

        (let [ae-event         (sut/new-anti-entropy-event node1)
              *expecting-event (promise)
              error-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :anti-entropy-event-bad-restart-counter-error)
                                     (deliver *expecting-event cmd))))]

          (add-tap error-catcher-fn)

          (m/assert 1 (count (sut/get-neighbours node2)))

          ;; sending anti-entropy event to node 2 with outdated restart counter
          (sut/send-event node1 ae-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/assert :anti-entropy-event-bad-restart-counter-error @*expecting-event)
          (m/assert 1 (count (sut/get-neighbours node2)))

          (remove-tap error-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do nothing if event contains bad tx"
    (let [node1 (sut/new-node-object node-data1 cluster)
          node2 (sut/new-node-object node-data2 cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node
                                      (assoc neighbour-data3 :tx 999)))

        (let [ae-event         (sut/new-anti-entropy-event node1)
              *expecting-event (promise)
              error-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :anti-entropy-event-bad-tx-error)
                                     (deliver *expecting-event cmd))))]

          (add-tap error-catcher-fn)

          (m/assert 1 (count (sut/get-neighbours node2)))

          ;; sending anti-entropy event to node 2 with outdated tx
          (sut/send-event node1 ae-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/assert :anti-entropy-event-bad-tx-error @*expecting-event)
          (m/assert 1 (count (sut/get-neighbours node2)))

          (remove-tap error-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do not process events from not alive nodes"
    (let [node1 (sut/new-node-object node-data1 cluster)
          node2 (sut/new-node-object node-data2 cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node
                                      (assoc neighbour-data3 :status :stop)))

        (let [ae-event         (sut/new-anti-entropy-event node1)
              *expecting-event (promise)
              error-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :anti-entropy-event-not-alive-neighbour-error)
                                     (deliver *expecting-event cmd))))]

          (add-tap error-catcher-fn)

          (m/assert 1 (count (sut/get-neighbours node2)))

          ;; sending anti-entropy event to node 2 from not alive neighbour
          (sut/send-event node1 ae-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/assert :anti-entropy-event-not-alive-neighbour-error @*expecting-event)
          (m/assert 1 (count (sut/get-neighbours node2)))

          (remove-tap error-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2))))))


(deftest ^:logic indirect-ack-event-test

  (testing "Accept normal indirect ack event on node1 from node3 via node2"
    (let [node1           (sut/new-node-object node-data1 cluster)
          node2           (sut/new-node-object node-data2 cluster)
          node3           (sut/new-node-object node-data3 cluster)
          intermediate-id (sut/get-id node2)
          neighbour-id    (sut/get-id node3)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node3 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))

        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))

        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data3))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data1))

        (let [indirect-ping      (sut/new-indirect-ping-event node1 intermediate-id neighbour-id 1)
              _                  (sut/upsert-indirect-ping node1 indirect-ping)
              indirect-ack-event (sut/new-indirect-ack-event node3 indirect-ping)
              *expecting-event   (promise)
              event-catcher-fn   (fn [v]
                                   (when-let [cmd (:org.rssys.swim/cmd v)]
                                     (when (= cmd :indirect-ack-event)
                                       (deliver *expecting-event v))))
              *expecting-event2  (promise)
              event-catcher-fn2  (fn [v]
                                   (when-let [cmd (:org.rssys.swim/cmd v)]
                                     (when (= cmd :intermediate-node-indirect-ack-event)
                                       (deliver *expecting-event2 v))))
              *expecting-event3  (promise)
              event-catcher-fn3  (fn [v]
                                   (when-let [cmd (:org.rssys.swim/cmd v)]
                                     (when (= cmd :put-event)
                                       (deliver *expecting-event3 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)
          (add-tap event-catcher-fn3)

          (testing "indirect ping request exists in a table"
            (m/assert ^:matcho/strict
              {:attempt-number    1
               :cmd-type          14
               :host              "127.0.0.1"
               :id                #uuid "00000000-0000-0000-0000-000000000001"
               :intermediate-host "127.0.0.1"
               :intermediate-id   #uuid "00000000-0000-0000-0000-000000000002"
               :intermediate-port 5377
               :neighbour-host    "127.0.0.1"
               :neighbour-id      #uuid "00000000-0000-0000-0000-000000000003"
               :neighbour-port    5378
               :port              5376
               :restart-counter   8
               :tx                1}
              (sut/get-indirect-ping-event node1 neighbour-id)))

          ;; sending normal indirect ack event from node 3 to node 1 via node 2
          (sut/send-event node3 indirect-ack-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event2 *max-test-timeout* :timeout))
          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))

          (m/assert ^:matcho/strict
            {:org.rssys.swim/cmd :indirect-ack-event
             :node-id            #uuid "00000000-0000-0000-0000-000000000001"
             :data               {:attempt-number    1
                                  :cmd-type          15
                                  :host              "127.0.0.1"
                                  :id                #uuid "00000000-0000-0000-0000-000000000003"
                                  :intermediate-host "127.0.0.1"
                                  :intermediate-id   #uuid "00000000-0000-0000-0000-000000000002"
                                  :intermediate-port 5377
                                  :neighbour-host    "127.0.0.1"
                                  :neighbour-id      #uuid "00000000-0000-0000-0000-000000000001"
                                  :neighbour-port    5376
                                  :port              5378
                                  :restart-counter   7
                                  :status            :left
                                  :tx                1}}
            @*expecting-event)

          (testing "indirect ping request does not exists in a table"
            (m/assert nil (sut/get-indirect-ping-event node1 neighbour-id)))

          (testing "put event about alive node is success"
            (m/dessert :timeout (deref *expecting-event3 *max-test-timeout* :timeout))
            (m/assert ^:matcho/strict
              {:org.rssys.swim/cmd :put-event
               :node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :data               {:event
                                    {:cmd-type                  3
                                     :id                        #uuid "00000000-0000-0000-0000-000000000001"
                                     :neighbour-id              #uuid "00000000-0000-0000-0000-000000000003"
                                     :neighbour-restart-counter 7
                                     :neighbour-tx              1
                                     :restart-counter           8
                                     :tx                        3}
                                    :tx 4}}
              @*expecting-event3))

          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2)
          (remove-tap event-catcher-fn3))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Do not process indirect ack event from unknown neighbour"
    (let [node1           (sut/new-node-object node-data1 cluster)
          node2           (sut/new-node-object node-data2 cluster)
          node3           (sut/new-node-object node-data3 cluster)
          intermediate-id (sut/get-id node2)
          neighbour-id    (sut/get-id node3)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node3 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))

        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))

        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data3))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data1))

        (let [indirect-ping      (sut/new-indirect-ping-event node1 intermediate-id neighbour-id 1)
              _                  (sut/upsert-indirect-ping node1 indirect-ping)
              indirect-ack-event (sut/new-indirect-ack-event node3 indirect-ping)
              *expecting-event   (promise)
              event-catcher-fn   (fn [v]
                                   (when-let [cmd (:org.rssys.swim/cmd v)]
                                     (when (= cmd :indirect-ack-event-unknown-neighbour-error)
                                       (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          ;; remove node3 from known hosts
          (sut/delete-neighbour node1 neighbour-id)

          ;; sending indirect ack event from unknown node 3 to node 1 via node 2
          (sut/send-event node3 indirect-ack-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/assert ^:matcho/strict
            {:org.rssys.swim/cmd :indirect-ack-event-unknown-neighbour-error
             :node-id            #uuid "00000000-0000-0000-0000-000000000001"
             :data
             {:attempt-number    1
              :cmd-type          15
              :host              "127.0.0.1"
              :id                #uuid "00000000-0000-0000-0000-000000000003"
              :intermediate-host "127.0.0.1"
              :intermediate-id   #uuid "00000000-0000-0000-0000-000000000002"
              :intermediate-port 5377
              :neighbour-host    "127.0.0.1"
              :neighbour-id      #uuid "00000000-0000-0000-0000-000000000001"
              :neighbour-port    5376
              :port              5378
              :restart-counter   7
              :status            :left
              :tx                1}}
            @*expecting-event)

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Do not accept indirect ack event on node1 from node3 with outdated restart counter"
    (let [node1           (sut/new-node-object node-data1 cluster)
          node2           (sut/new-node-object node-data2 cluster)
          node3           (sut/new-node-object node-data3 cluster)
          intermediate-id (sut/get-id node2)
          neighbour-id    (sut/get-id node3)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node3 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data2 :restart-counter 999)))

        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))

        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data3))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data1))

        (let [indirect-ping      (sut/new-indirect-ping-event node1 intermediate-id neighbour-id 1)
              _                  (sut/upsert-indirect-ping node1 indirect-ping)
              indirect-ack-event (sut/new-indirect-ack-event node3 indirect-ping)
              *expecting-event   (promise)
              event-catcher-fn   (fn [v]
                                   (when-let [cmd (:org.rssys.swim/cmd v)]
                                     (when (= cmd :indirect-ack-event-bad-restart-counter-error)
                                       (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          ;; sending outdated indirect ack event from node 3 to node 1 via node 2
          (sut/send-event node3 indirect-ack-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))

          (m/assert ^:matcho/strict
            {:org.rssys.swim/cmd :indirect-ack-event-bad-restart-counter-error
             :node-id            #uuid "00000000-0000-0000-0000-000000000001"
             :data
             {:attempt-number    1
              :cmd-type          15
              :host              "127.0.0.1"
              :id                #uuid "00000000-0000-0000-0000-000000000003"
              :intermediate-host "127.0.0.1"
              :intermediate-id   #uuid "00000000-0000-0000-0000-000000000002"
              :intermediate-port 5377
              :neighbour-host    "127.0.0.1"
              :neighbour-id      #uuid "00000000-0000-0000-0000-000000000001"
              :neighbour-port    5376
              :port              5378
              :restart-counter   7
              :status            :left
              :tx                1}}
            @*expecting-event)

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Do not accept indirect ack event on node1 from node3 with outdated tx"
    (let [node1           (sut/new-node-object node-data1 cluster)
          node2           (sut/new-node-object node-data2 cluster)
          node3           (sut/new-node-object node-data3 cluster)
          intermediate-id (sut/get-id node2)
          neighbour-id    (sut/get-id node3)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node3 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data2 :tx 999)))

        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))

        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data3))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data1))

        (let [indirect-ping      (sut/new-indirect-ping-event node1 intermediate-id neighbour-id 1)
              _                  (sut/upsert-indirect-ping node1 indirect-ping)
              indirect-ack-event (sut/new-indirect-ack-event node3 indirect-ping)
              *expecting-event   (promise)
              event-catcher-fn   (fn [v]
                                   (when-let [cmd (:org.rssys.swim/cmd v)]
                                     (when (= cmd :indirect-ack-event-bad-tx-error)
                                       (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          ;; sending outdated indirect ack event from node 3 to node 1 via node 2
          (sut/send-event node3 indirect-ack-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))

          (m/assert ^:matcho/strict
            {:org.rssys.swim/cmd :indirect-ack-event-bad-tx-error
             :node-id            #uuid "00000000-0000-0000-0000-000000000001"
             :data
             {:attempt-number    1
              :cmd-type          15
              :host              "127.0.0.1"
              :id                #uuid "00000000-0000-0000-0000-000000000003"
              :intermediate-host "127.0.0.1"
              :intermediate-id   #uuid "00000000-0000-0000-0000-000000000002"
              :intermediate-port 5377
              :neighbour-host    "127.0.0.1"
              :neighbour-id      #uuid "00000000-0000-0000-0000-000000000001"
              :neighbour-port    5376
              :port              5378
              :restart-counter   7
              :status            :left
              :tx                1}}
            @*expecting-event)

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Do not process indirect ack event if indirect ping never sent"
    (let [node1           (sut/new-node-object node-data1 cluster)
          node2           (sut/new-node-object node-data2 cluster)
          node3           (sut/new-node-object node-data3 cluster)
          intermediate-id (sut/get-id node2)
          neighbour-id    (sut/get-id node3)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node3 empty-node-process-fn sut/incoming-udp-processor-fn)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))

        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))

        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data3))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node neighbour-data1))

        (let [indirect-ping      (sut/new-indirect-ping-event node1 intermediate-id neighbour-id 1)
              ;; here we not put this ping into the indirect ping table on node1
              indirect-ack-event (sut/new-indirect-ack-event node3 indirect-ping)
              *expecting-event   (promise)
              event-catcher-fn   (fn [v]
                                   (when-let [cmd (:org.rssys.swim/cmd v)]
                                     (when (= cmd :indirect-ack-event-not-expected-error)
                                       (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          ;; sending indirect ack event from node 3 to node 1 via node 2
          (sut/send-event node3 indirect-ack-event (sut/get-host node2) (sut/get-port node2))

          (m/dessert :timeout (deref *expecting-event *max-test-timeout* :timeout))
          (m/assert ^:matcho/strict
            {:org.rssys.swim/cmd :indirect-ack-event-not-expected-error
             :node-id            #uuid "00000000-0000-0000-0000-000000000001"
             :data
             {:attempt-number    1
              :cmd-type          15
              :host              "127.0.0.1"
              :id                #uuid "00000000-0000-0000-0000-000000000003"
              :intermediate-host "127.0.0.1"
              :intermediate-id   #uuid "00000000-0000-0000-0000-000000000002"
              :intermediate-port 5377
              :neighbour-host    "127.0.0.1"
              :neighbour-id      #uuid "00000000-0000-0000-0000-000000000001"
              :neighbour-port    5376
              :port              5378
              :restart-counter   7
              :status            :left
              :tx                1}}
            @*expecting-event)

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3))))))


#_(deftest ^:logic ack-event-test

    (testing "Don't process ack event from unknown neighbour"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ack-event-unknown-neighbour-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          ;; sending ack event to node 1 from unknown neighbour
          (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
            (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (match @*expecting-error :ack-event-unknown-neighbour-error)
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Don't process ack event with outdated restart counter"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ack-event-bad-restart-counter-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :restart-counter 999)))

          ;; sending ack event to node 1 from with outdated restart counter
          (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
            (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (match @*expecting-error :ack-event-bad-restart-counter-error)
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))


    (testing "Don't process ack event with outdated tx"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ack-event-bad-tx-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :tx 999)))

          ;; sending ack event to node 1 from with outdated tx
          (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
            (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (match @*expecting-error :ack-event-bad-tx-error)
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))


    (testing "Don't process ack event from not alive nodes"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ack-event-not-alive-neighbour-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :status :dead)))

          ;; sending ack event to node 1 from dead neighbour
          (sut/inc-tx node2)
          (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
            (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (match @*expecting-error :ack-event-not-alive-neighbour-error)
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))


    (testing "Don't process ack event if ack is not requested"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ack-event-no-active-ping-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))

          ;; sending ack event to node 1 without ping
          (sut/inc-tx node2)
          (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
            (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (match @*expecting-error :ack-event-no-active-ping-error)
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Process normal ack from alive node"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            *expecting-event (promise)
            event-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ack-event)
                                   (deliver *expecting-event cmd))))]
        (try
          (add-tap event-catcher-fn)                        ;; register event catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))

          (sut/upsert-ping node1 (sut/new-ping-event node1 (sut/get-id node2) 1))
          ;; sending normal ack event to node 1
          (sut/inc-tx node2)
          (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
            (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and event-catcher-fn
          (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
          (match @*expecting-event :ack-event)
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap event-catcher-fn)                   ;; unregister event catcher
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Process normal ack from suspect node"
      (let [node1             (sut/new-node-object node-data1 cluster)
            node2             (sut/new-node-object node-data2 cluster)
            *expecting-event  (promise)
            *expecting-event2 (promise)
            *expecting-event3 (promise)
            event-catcher-fn  (fn [v]
                                (when-let [cmd (:org.rssys.swim/cmd v)]
                                  (when (= cmd :put-event)
                                    (deliver *expecting-event cmd))
                                  (when (= cmd :alive-event)
                                    (deliver *expecting-event2 cmd))
                                  (when (= cmd :ack-event)
                                    (deliver *expecting-event3 cmd))))]
        (try
          (add-tap event-catcher-fn)                        ;; register event catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :status :suspect)))

          (sut/upsert-ping node1 (sut/new-ping-event node1 (sut/get-id node2) 1))
          ;; sending normal ack event to node 1 from suspect node
          (sut/inc-tx node2)
          (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
            (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and event-catcher-fn
          (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
          (not-match (deref *expecting-event2 *max-test-timeout* :timeout) :timeout)
          (not-match (deref *expecting-event3 *max-test-timeout* :timeout) :timeout)
          (match
            [@*expecting-event @*expecting-event2 @*expecting-event3]
            [:put-event :alive-event :ack-event])
          (match (-> node1 sut/get-outgoing-events first type) AliveEvent) ;; we send new alive event
          (match (count (sut/get-alive-neighbours node1)) 1) ;; we set new :alive status
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap event-catcher-fn)                   ;; unregister event catcher
            (sut/stop node1)
            (sut/stop node2))))))





#_(deftest ^:logic ping-event-test

    (testing "Don't process ping event for different addressee"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            ping-event       (sut/new-ping-event node2 (sut/get-id node1) 1)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ping-event-different-addressee-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          ;; sending ping event to node 1 for different addressee (another id)
          (sut/send-event node2 (assoc ping-event :neighbour-id (UUID. 0 99)) (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (when (realized? *expecting-error)
            (match @*expecting-error :ping-event-different-addressee-error))
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))


    (testing "Don't process ping event from unknown neighbour"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            ping-event       (sut/new-ping-event node2 (sut/get-id node1) 1)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ping-event-unknown-neighbour-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          ;; sending ack event to node 1 from unknown neighbour
          (sut/send-event node2 ping-event (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (when (realized? *expecting-error)
            (match @*expecting-error :ping-event-unknown-neighbour-error))
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Don't process ping event with outdated restart counter"
      (let [node1             (sut/new-node-object node-data1 cluster)
            node2             (sut/new-node-object node-data2 cluster)
            ping-event        (sut/new-ping-event node2 (sut/get-id node1) 1)
            *expecting-error  (promise)
            *expecting-error2 (promise)
            error-catcher-fn  (fn [v]
                                (when-let [cmd (:org.rssys.swim/cmd v)]
                                  (when (= cmd :ping-event-bad-restart-counter-error)
                                    (deliver *expecting-error cmd))))
            error-catcher-fn2 (fn [v]
                                (when-let [cmd (:org.rssys.swim/cmd v)]
                                  (when (= cmd :ping-event-reply-dead-event)
                                    (deliver *expecting-error2 v))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (add-tap error-catcher-fn2)                       ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :restart-counter 999)))

          ;; sending ping event to node 1 from node 2 with outdated restart counter
          (sut/send-event node2 ping-event (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (when (realized? *expecting-error)
            (match @*expecting-error :ping-event-bad-restart-counter-error))

          (testing "node 1 should send dead event to node 2"
            (not-match (deref *expecting-error2 *max-test-timeout* :timeout) :timeout)
            (when (realized? *expecting-error2)
              (match @*expecting-error2 {:org.rssys.swim/cmd :ping-event-reply-dead-event
                                         :node-id            #uuid "00000000-0000-0000-0000-000000000001"
                                         :data
                                         #org.rssys.event.DeadEvent{:cmd-type        6
                                                                    :id              #uuid "00000000-0000-0000-0000-000000000001"
                                                                    :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                                                                    :neighbour-tx    0
                                                                    :restart-counter 8
                                                                    :tx              2}})))

          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (remove-tap error-catcher-fn2)                  ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))


    (testing "Don't process ping event with outdated tx"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            ping-event       (sut/new-ping-event node2 (sut/get-id node1) 1)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ping-event-bad-tx-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :tx 999)))

          ;; sending ping event to node 1 from node 2 with outdated tx
          (sut/send-event node2 ping-event (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (when (realized? *expecting-error)
            (match @*expecting-error :ping-event-bad-tx-error))
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Don't process ping event from not alive nodes"
      (let [node1            (sut/new-node-object node-data1 cluster)
            node2            (sut/new-node-object node-data2 cluster)
            *expecting-error (promise)
            error-catcher-fn (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (= cmd :ping-event-not-alive-neighbour-error)
                                   (deliver *expecting-error cmd))))]
        (try
          (add-tap error-catcher-fn)                        ;; register error catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :status :dead)))

          ;; sending ping event to node 1 from the dead neighbour

          (sut/send-event node2 (sut/new-ping-event node2 (sut/get-id node1) 1)
            (sut/get-host node1) (sut/get-port node1))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-error *max-test-timeout* :timeout) :timeout)
          (when (realized? *expecting-error)
            (match @*expecting-error :ping-event-not-alive-neighbour-error))
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap error-catcher-fn)                   ;; unregister error catcher
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "Process normal ping from alive node"
      (let [node1             (sut/new-node-object node-data1 cluster)
            node2             (sut/new-node-object node-data2 cluster)
            *expecting-event  (promise)
            *expecting-event2 (promise)
            event-catcher-fn  (fn [v]
                                (when-let [cmd (:org.rssys.swim/cmd v)]
                                  (when (= cmd :ping-event-ack-event)
                                    (deliver *expecting-event cmd))))
            event-catcher-fn2 (fn [v]
                                (when-let [cmd (:org.rssys.swim/cmd v)]
                                  (when (= cmd :ack-event)
                                    (deliver *expecting-event2 cmd))))]
        (try
          (add-tap event-catcher-fn)                        ;; register event catcher
          (add-tap event-catcher-fn2)                       ;; register event catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc neighbour-data3 :status :alive)))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :status :alive)))

          ;; sending ping event to node 1 from alive node 2

          (testing "Ping event on node 1 is received and processed correctly"
            (let [ping-event (sut/new-ping-event node2 (sut/get-id node1) 1)]
              (sut/upsert-ping node2 ping-event)
              (sut/send-event node2 ping-event (sut/get-host node1) (sut/get-port node1))))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
          (when (realized? *expecting-event)
            (match @*expecting-event :ping-event-ack-event))

          (testing "AckEvent on node 2 is received and processed correctly"
            (not-match (deref *expecting-event2 *max-test-timeout* :timeout) :timeout)
            (when (realized? *expecting-event2)
              (match @*expecting-event2 :ack-event)))

          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap event-catcher-fn)                   ;; unregister event catcher
            (remove-tap event-catcher-fn2)                  ;; unregister event catcher
            (sut/stop node1)
            (sut/stop node2)))))


    (testing "Process normal ping from suspect node"
      (let [node1             (sut/new-node-object node-data1 cluster)
            node2             (sut/new-node-object node-data2 cluster)
            *expecting-event  (promise)
            *expecting-event2 (promise)
            *expecting-event3 (promise)
            event-catcher-fn  (fn [v]
                                (when-let [cmd (:org.rssys.swim/cmd v)]
                                  (when (= cmd :ping-event-ack-event)
                                    (deliver *expecting-event cmd))))
            event-catcher-fn2 (fn [v]
                                (when-let [cmd (:org.rssys.swim/cmd v)]
                                  (when (= cmd :ack-event)
                                    (deliver *expecting-event2 cmd))))
            event-catcher-fn3 (fn [v]
                                (when-let [cmd (:org.rssys.swim/cmd v)]
                                  (when (= cmd :alive-event)
                                    (deliver *expecting-event3 v))))]
        (try
          (add-tap event-catcher-fn)                        ;; register event catcher
          (add-tap event-catcher-fn2)                       ;; register event catcher
          (add-tap event-catcher-fn3)                       ;; register event catcher
          (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
          (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc neighbour-data3 :status :alive)))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :status :suspect)))

          ;; sending ping event to node 1 from suspect node 2

          (testing "Ping event on node 1 is received and processed correctly"
            (let [ping-event (sut/new-ping-event node2 (sut/get-id node1) 1)]
              (sut/upsert-ping node2 ping-event)
              (sut/send-event node2 ping-event (sut/get-host node1) (sut/get-port node1))))
          ;; wait for event processing and error-catcher-fn
          (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
          (when (realized? *expecting-event)
            (match @*expecting-event :ping-event-ack-event))

          (testing "AckEvent on node 2 is received and processed correctly"
            (not-match (deref *expecting-event2 *max-test-timeout* :timeout) :timeout)
            (when (realized? *expecting-event2)
              (match @*expecting-event2 :ack-event)))

          (testing "node 1 should create new event that node 2 is now alive"
            (not-match (deref *expecting-event3 *max-test-timeout* :timeout) :timeout)
            (when (realized? *expecting-event3)
              (match @*expecting-event3 {:org.rssys.swim/cmd :alive-event
                                         :node-id            #uuid "00000000-0000-0000-0000-000000000001"
                                         :data
                                         {:neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                                          :previous-status :suspect}})))

          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap event-catcher-fn)                   ;; unregister event catcher
            (remove-tap event-catcher-fn2)                  ;; unregister event catcher
            (remove-tap event-catcher-fn3)                  ;; unregister event catcher
            (sut/stop node1)
            (sut/stop node2))))))







