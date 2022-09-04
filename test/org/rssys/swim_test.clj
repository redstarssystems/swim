(ns org.rssys.swim-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as string]
    [clojure.test :refer [deftest is testing]]
    [matcho.core :refer [match]]
    [org.rssys.encrypt :as e]
    [org.rssys.event :as event]
    [org.rssys.spec :as spec]
    [org.rssys.swim :as sut])
  (:import
    (org.rssys.domain
      Cluster
      NeighbourNode
      NodeObject)
    (org.rssys.event
      AckEvent
      AliveEvent
      AntiEntropy
      DeadEvent
      PingEvent
      ProbeAckEvent
      ProbeEvent)
    (org.rssys.scheduler
      MutablePool)))



(deftest safe-test
  (is (= nil (sut/safe (/ 1 0))) "Any Exceptions should be prevented")
  (is (= 1/2 (sut/safe (/ 1 2)))) "Any normal expression should be succeed")


(deftest calc-n-test
  (testing "How many nodes should we notify depending on N nodes in a cluster"
    (let [nodes-in-cluster [1 2 4 8 16 32 64 128 256 512 1024]
          result           (mapv sut/calc-n nodes-in-cluster)]
      (is (= [0 1 2 3 4 5 6 7 8 9 10] result)))))


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
      (match dvalues values))))




;;;;;;;;;;

(def cluster-data
  {:id           #uuid "10000000-0000-0000-0000-000000000000"
   :name         "cluster1"
   :desc         "Test cluster1"
   :secret-token "0123456789abcdef0123456789abcdef"
   :nspace       "test-ns1"
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
      (is (instance? Cluster result) "Should be Cluster instance")
      (is (s/valid? ::spec/cluster result))
      (is (= 1 (.-cluster_size result))))))


;;;;


(deftest new-neighbour-node-test
  (testing "Create NeighbourNode instance is successful"
    (let [result1 (sut/new-neighbour-node neighbour-data1)
          result2 (sut/new-neighbour-node #uuid "00000000-0000-0000-0000-000000000000" "127.0.0.1" 5379)]
      (is (instance? NeighbourNode result1) "Should be NeighbourNode instance")
      (is (instance? NeighbourNode result2) "Should be NeighbourNode instance")
      (is (s/valid? ::spec/neighbour-node result1) "NeighbourNode should correspond to spec")
      (is (s/valid? ::spec/neighbour-node result2) "NeighbourNode should correspond to spec")
      (testing "NeighbourNode has expected keys and values"
        (match result1 neighbour-data1))
      (match result2 {:id              #uuid "00000000-0000-0000-0000-000000000000"
                      :host            "127.0.0.1"
                      :port            5379
                      :status          :unknown
                      :access          :direct
                      :restart-counter 0
                      :tx              0
                      :payload         {}
                      :updated-at      nat-int?}))))


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
        (match (sut/get-value node-object)
          {:id                   #uuid "00000000-0000-0000-0000-000000000001"
           :host                 "127.0.0.1"
           :port                 5376
           :cluster              cluster
           :status               :stop
           :neighbours           {}
           :restart-counter      0
           :tx                   0
           :ping-events          {}
           :payload              {}
           :outgoing-event-queue []
           :ping-round-buffer    []
           :scheduler-pool       #(instance? MutablePool %)
           :*udp-server          nil}))))

  (testing "Wrong node data is prohibited"
    (is (thrown-with-msg? Exception #"Invalid node data"
          (sut/new-node-object {:a 1} cluster)))))


(sut/get-cluster (sut/new-node-object node-data1 cluster))


(deftest getters-test
  (testing "getters"
    (let [this (sut/new-node-object node-data1 cluster)]

      (match (sut/get-value this)
        {:id                   #uuid "00000000-0000-0000-0000-000000000001"
         :host                 "127.0.0.1"
         :port                 5376
         :cluster              cluster
         :status               :stop
         :neighbours           {}
         :restart-counter      7
         :tx                   0
         :ping-events          {}
         :payload              {}
         :outgoing-event-queue []
         :ping-round-buffer    []
         :scheduler-pool       #(instance? MutablePool %)
         :*udp-server          nil})

      (match (sut/get-id this) #uuid "00000000-0000-0000-0000-000000000001")
      (match (sut/get-host this) "127.0.0.1")
      (match (sut/get-port this) 5376)
      (match (sut/get-restart-counter this) 7)
      (match (sut/get-tx this) 0)
      (match (sut/get-cluster this) cluster)
      (match (sut/get-cluster-size this) 1)
      (match (sut/get-payload this) empty?)

      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))

      (match (sut/get-neighbours this)
        {#uuid "00000000-0000-0000-0000-000000000002"
         (dissoc (sut/new-neighbour-node neighbour-data1) :updated-at)})

      (match (sut/get-neighbour this #uuid "00000000-0000-0000-0000-000000000002")
        {:id              #uuid "00000000-0000-0000-0000-000000000002"
         :host            "127.0.0.1"
         :port            5377
         :status          :alive
         :access          :direct
         :payload         {:tcp-port 4567}
         :restart-counter 3
         :tx              0})

      (match (sut/get-status this) :stop)
      (match (sut/get-outgoing-event-queue this) empty?)

      (sut/upsert-ping this (event/empty-ping))

      (match (sut/get-ping-events this)
        {#uuid"00000000-0000-0000-0000-000000000000"
         {:cmd-type        0
          :id              #uuid"00000000-0000-0000-0000-000000000000"
          :host            "localhost"
          :port            1
          :restart-counter 0
          :tx              0
          :neighbour-id    #uuid"00000000-0000-0000-0000-000000000000"
          :attempt-number  1}})

      (match (sut/get-ping-event this #uuid"00000000-0000-0000-0000-000000000000")
        {:cmd-type        0
         :id              #uuid"00000000-0000-0000-0000-000000000000"
         :host            "localhost"
         :port            1
         :restart-counter 0
         :tx              0
         :neighbour-id    #uuid"00000000-0000-0000-0000-000000000000"
         :attempt-number  1}))))


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
      (match (sut/get-cluster this) new-cluster)
      (is (thrown-with-msg? Exception #"Invalid cluster data"
            (sut/set-cluster this (assoc cluster :id 1))))
      (testing "Cluster change allowed only in status :stop"
        (is (thrown-with-msg? Exception #"Node is not stopped"
              (sut/set-status this :left)
              (sut/set-cluster this new-cluster)))))))


(deftest set-cluster-size-test
  (testing "set cluster size"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-cluster-size this) 1)
      (sut/set-cluster-size this 3)
      (match (sut/get-cluster-size this) 3)
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid cluster size"
              (sut/set-cluster-size this -1)))))))


(deftest set-status-test
  (testing "set status"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-status this) :stop)
      (match (sut/set-status this :left))
      (match (sut/get-status this) :left)
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid node status"
              (sut/set-status this :wrong-value)))))))


(deftest set-payload-test
  (testing "set payload"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-payload this) empty?)
      (sut/set-payload this {:tcp-port 1234 :role "data node"})
      (match (sut/get-payload this) {:tcp-port 1234 :role "data node"})
      (testing "too big payload cannot be set"
        (is (thrown-with-msg? Exception #"Size of payload is too big"
              (sut/set-payload this {:long-string (apply str (repeat 1024 "a"))})))))))


(deftest set-restart-counter-test
  (testing "set restart counter"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-restart-counter this) 7)
      (sut/set-restart-counter this 42)
      (match (sut/get-restart-counter this) 42)
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid restart counter data"
              (sut/set-restart-counter this -1)))))))


(deftest inc-tx-test
  (testing "increment tx"
    (let [new-cluster (sut/new-cluster (assoc cluster-data :id (random-uuid) :name "cluster2"))
          this        (sut/new-node-object node-data1 cluster)]
      (match (sut/get-tx this) 0)
      (sut/inc-tx this)
      (match (sut/get-tx this) 1))))


(deftest upsert-neighbour-test
  (testing "upsert neighbour"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-neighbours this) empty?)
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data2))
      (match (sut/get-neighbour this (:id neighbour-data1)) (dissoc neighbour-data1 :updated-at))
      (match (count (sut/get-neighbours this)) 2)
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid neighbour node data"
              (sut/upsert-neighbour this {:a :bad-value}))))
      (testing "Update timestamp after every neighbour update"
        (let [nb        (sut/get-neighbour this (:id neighbour-data1))
              timestamp (:updated-at nb)
              port      (:port nb)]
          (match timestamp pos-int?)
          (match port pos-int?)
          (sut/upsert-neighbour this (sut/new-neighbour-node (assoc neighbour-data1 :port 5370)))
          (let [modified-nb (sut/get-neighbour this (:id neighbour-data1))]
            (is (not= timestamp (:updated-at modified-nb)))
            (is (not= port (:port modified-nb)))))))))


(deftest delete-neighbour-test
  (testing "delete neighbour"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-neighbours this) empty?)
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data2))
      (match (count (sut/get-neighbours this)) 2)
      (sut/delete-neighbour this (:id neighbour-data1))
      (match (count (sut/get-neighbours this)) 1)
      (match (sut/get-neighbour this (:id neighbour-data2)) (dissoc neighbour-data2 :updated-at)))))


(deftest set-outgoing-event-queue-test
  (testing "set outgoing event queue"
    (let [new-event-queue [[(:left event/code) (random-uuid)]]
          this            (sut/new-node-object node-data1 cluster)]
      (match (sut/get-outgoing-event-queue this) empty?)
      (sut/set-outgoing-event-queue this new-event-queue)
      (match (sut/get-outgoing-event-queue this) new-event-queue))))


(deftest put-event-test
  (testing "put event"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-outgoing-event-queue this) empty?)
      (sut/put-event this (event/empty-ping))
      (sut/put-event this (event/empty-ack))
      (match (sut/get-outgoing-event-queue this) [(event/empty-ping) (event/empty-ack)])
      (match (count (sut/get-outgoing-event-queue this)) 2))))


(deftest take-event-test
  (testing "take event"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-outgoing-event-queue this) empty?)
      (sut/put-event this (event/empty-ping))
      (sut/put-event this (event/empty-ack))
      (match (sut/take-event this) (event/empty-ping))
      (match (count (sut/get-outgoing-event-queue this)) 1)
      (match (sut/take-event this) (event/empty-ack)))))


(deftest take-events-test
  (testing "take events"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-outgoing-event-queue this) empty?)
      (sut/put-event this (event/empty-ping))
      (sut/put-event this (event/empty-ack))
      (match (count (sut/get-outgoing-event-queue this)) 2)
      (match (sut/take-events this 2) [(event/empty-ping) (event/empty-ack)])
      (match (count (sut/get-outgoing-event-queue this)) 0)))
  (testing "take all events"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-outgoing-event-queue this) empty?)
      (sut/put-event this (event/empty-ping))
      (sut/put-event this (event/empty-ack))
      (match (count (sut/get-outgoing-event-queue this)) 2)
      (match (sut/take-events this) [(event/empty-ping) (event/empty-ack)])
      (match (count (sut/get-outgoing-event-queue this)) 0))))


(deftest upsert-ping-test
  (testing "upsert ping"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-ping-events this) empty?)
      (sut/upsert-ping this (event/empty-ping))
      (match (count (sut/get-ping-events this)) 1)
      (match (sut/get-ping-events this) {#uuid "00000000-0000-0000-0000-000000000000"
                                         (event/empty-ping)})
      (testing "Wrong data is prohibited by spec"
        (is (thrown-with-msg? Exception #"Invalid ping event data"
              (sut/upsert-ping this {:a :bad-value})))))))


(deftest delete-ping-test
  (testing "delete ping"
    (let [this (sut/new-node-object node-data1 cluster)]
      (match (sut/get-ping-events this) empty?)
      (sut/upsert-ping this (event/empty-ping))
      (match (count (sut/get-ping-events this)) 1)
      (sut/delete-ping this #uuid "00000000-0000-0000-0000-000000000000")
      (match (count (sut/get-ping-events this)) zero?))))


;;;;


(deftest new-ping-event-test
  (let [this   (sut/new-node-object node-data1 cluster)
        result (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)]
    (is (= PingEvent (type result)) "PingEvent has correct type")
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid ping event"
            (sut/new-ping-event this :bad-value 42))))))


(deftest new-ack-event-test
  (let [this       (sut/new-node-object node-data1 cluster)
        ping-event (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)
        result     (sut/new-ack-event this ping-event)]
    (is (= AckEvent (type result)) "PingEvent has correct type")
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid ack event"
            (sut/new-ack-event this (assoc ping-event :id :bad-value)))))))


(deftest new-dead-event-test
  (let [this       (sut/new-node-object node-data1 cluster)
        ping-event (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)
        result     (sut/new-dead-event this ping-event)]
    (is (= DeadEvent (type result)) "DeadEvent has correct type")
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid dead event"
            (sut/new-dead-event this (assoc ping-event :id :bad-value)))))))


(deftest new-probe-event-test
  (let [this        (sut/new-node-object node-data1 cluster)
        probe-event (sut/new-probe-event this "127.0.0.1" 5376)]
    (is (= ProbeEvent (type probe-event)) "ProbeEvent has correct type")
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid probe event"
            (sut/new-probe-event this "127.0.01" -1))))))


(deftest new-probe-ack-event-test
  (let [this            (sut/new-node-object node-data1 cluster)
        probe-event     (sut/new-probe-event this "127.0.0.1" 5376)
        probe-ack-event (sut/new-probe-ack-event this probe-event)]
    (is (= ProbeAckEvent (type probe-ack-event)) "ProbeAckEvent has correct type")
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid probe ack event"
            (sut/new-probe-ack-event this (assoc probe-event :id :bad-value)))))))


(deftest new-anti-entropy-event-test
  (let [this   (sut/new-node-object node-data1 cluster)
        result (sut/new-anti-entropy-event this)]
    (is (= AntiEntropy (type result)) "AntiEntropy has correct type")))


;;;;


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
      (sut/upsert-neighbour node1 (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000004"
                                                           :host            "127.0.0.1"
                                                           :port            5434
                                                           :status          :alive
                                                           :access          :direct
                                                           :restart-counter 4
                                                           :tx              4
                                                           :payload         {}
                                                           :updated-at      (System/currentTimeMillis)}))
      (sut/upsert-neighbour node1 (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000005"
                                                           :host            "127.0.0.1"
                                                           :port            5434
                                                           :status          :alive
                                                           :access          :direct
                                                           :restart-counter 5
                                                           :tx              5
                                                           :payload         {}
                                                           :updated-at      (System/currentTimeMillis)}))

      (is (= 2 (count (sut/build-anti-entropy-data node1))) "Default size of vector as expected")
      (is (= 4 (count (sut/build-anti-entropy-data node1 :num 4))) "Size of vector as requested")
      (is (= [] (sut/build-anti-entropy-data (sut/new-node-object node-data2 cluster)))
        "Empty neighbours map should produce empty anti-entropy vector")
      (is (every? #(instance? NeighbourNode (sut/new-neighbour-node %)) (sut/build-anti-entropy-data node1))
        "Every value in vector is NeighbourNode"))))


;;;;


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
    (let [this        (sut/new-node-object node-data1 cluster)
          node2       (sut/new-node-object node-data2 cluster)
          probe-event (sut/new-probe-event this (sut/get-host node2) (sut/get-port node2))]
      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start this empty-node-process-fn #(do [%1 %2]))

        (testing "send event using host and port"
          (sut/send-event this probe-event (sut/get-host node2) (sut/get-port node2))
          (Thread/sleep 20)
          (match (sut/get-payload node2) [(.prepare probe-event)]))

        (testing "send event using neighbour id"
          (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
          (sut/send-event this (event/empty-ack) (sut/get-id node2))
          (Thread/sleep 20)
          (match (sut/get-payload node2) [(.prepare (event/empty-ack))]))

        (testing "Wrong neighbour id is prohibited"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/send-event this (event/empty-ack) (random-uuid)))))

        (testing "Too big UDP packet is prohibited"
          (binding [sut/*max-anti-entropy-items* 100]       ;; increase from 2 to 100
            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (dotimes [n 100]                          ;; fill too many neighbours
                    (sut/upsert-neighbour this (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))
                  (sut/send-event this (sut/new-anti-entropy-event this) (sut/get-id node2))))))


        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop this)
          (sut/stop node2))))))


(deftest send-event-ae-test
  (testing "node1 can send event to node2"
    (let [this        (sut/new-node-object node-data1 cluster)
          node2       (sut/new-node-object node-data2 cluster)
          probe-event (sut/new-probe-event this (sut/get-host node2) (sut/get-port node2))]
      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start this empty-node-process-fn #(do [%1 %2]))

        (testing "send event using host and port"
          (sut/send-event-ae this probe-event (sut/get-host node2) (sut/get-port node2))
          (Thread/sleep 20)
          (match (sut/get-payload node2) [(.prepare probe-event) (.prepare (event/empty-anti-entropy))]))

        (testing "send event using neighbour id"
          (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
          (sut/send-event-ae this (event/empty-ack) (sut/get-id node2))
          (Thread/sleep 20)
          (match (sut/get-payload node2) [(.prepare (event/empty-ack)) (.prepare (event/empty-anti-entropy))]))

        (testing "Wrong neighbour id is prohibited"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/send-event-ae this (event/empty-ack) (random-uuid)))))

        (testing "Too big UDP packet is prohibited"
          (binding [sut/*max-anti-entropy-items* 100]       ;; increase from 2 to 100
            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (dotimes [n 100]                          ;; fill too many neighbours
                    (sut/upsert-neighbour this (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))
                  (sut/send-event-ae this (sut/new-anti-entropy-event this) (sut/get-id node2))))))

        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop this)
          (sut/stop node2))))))


(deftest send-queue-events-test
  (testing "node1 can send all events to node2 with anti-entropy data"
    (let [this        (sut/new-node-object node-data1 cluster)
          node2       (sut/new-node-object node-data2 cluster)
          probe-event (sut/new-probe-event this (sut/get-host node2) (sut/get-port node2))]

      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start this empty-node-process-fn #(do [%1 %2]))

        (testing "send all events using host and port"
          (sut/put-event this probe-event)
          (sut/put-event this (event/empty-ack))
          (sut/send-queue-events this (sut/get-host node2) (sut/get-port node2))
          (Thread/sleep 20)
          (match (sut/get-payload node2)
            [(.prepare probe-event) (.prepare (event/empty-ack)) (.prepare (event/empty-anti-entropy))])
          (is (empty? (sut/get-outgoing-event-queue this)) "After send outgoing queue should be empty"))

        (testing "send all events using neighbour id"
          (sut/put-event this probe-event)
          (sut/put-event this (event/empty-ack))
          (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
          (sut/send-queue-events this (sut/get-id node2))
          (Thread/sleep 20)
          (match (sut/get-payload node2)
            [(.prepare probe-event) (.prepare (event/empty-ack)) (.prepare (event/empty-anti-entropy))])
          (is (empty? (sut/get-outgoing-event-queue this)) "After send outgoing queue should be empty"))

        (testing "Wrong neighbour id is prohibited"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/put-event this probe-event)
                (sut/send-queue-events this (random-uuid)))))

        (testing "Too big UDP packet is prohibited"
          (binding [sut/*max-anti-entropy-items* 100]       ;; increase from 2 to 100
            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (dotimes [n 100]                          ;; fill too many neighbours
                    (sut/upsert-neighbour this (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))
                  (sut/send-queue-events this (sut/get-id node2))))))

        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop this)
          (sut/stop node2))))))


(deftest get-neighbours-with-status-test
  (let [this (sut/new-node-object node-data1 cluster)
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
        (match (count result) 3)))

    (testing "Get all alive neighbours"
      (let [result (sut/get-alive-neighbours this)]
        (match (count result) 2)))

    (testing "Get all stopped neighbours"
      (let [result (sut/get-stopped-neighbours this)]
        (match (count result) 1)))

    (testing "Get all dead neighbours"
      (let [result (sut/get-dead-neighbours this)]
        (match (count result) 3)))

    (testing "Get all left neighbours"
      (let [result (sut/get-left-neighbours this)]
        (match (count result) 1)))

    (testing "Get all suspect neighbours"
      (let [result (sut/get-suspect-neighbours this)]
        (match (count result) 4)))))


(deftest get-oldest-neighbour-test
  (let [this (sut/new-node-object node-data1 cluster)
        nb0  (sut/new-neighbour-node (assoc neighbour-data2 :id #uuid"00000000-0000-0000-0000-000000000000"))
        nb1  (sut/new-neighbour-node (assoc neighbour-data2 :id #uuid"00000000-0000-0000-0000-000000000001"))
        nb2  (sut/new-neighbour-node (assoc neighbour-data2 :id #uuid"00000000-0000-0000-0000-000000000002"))
        nb3  (sut/new-neighbour-node (assoc neighbour-data2 :status :left :id #uuid"00000000-0000-0000-0000-000000000003"))]
    (sut/upsert-neighbour this nb0)
    (Thread/sleep 1)
    (sut/upsert-neighbour this nb1)
    (Thread/sleep 1)
    (sut/upsert-neighbour this nb2)
    (Thread/sleep 1)
    (sut/upsert-neighbour this nb3)
    (match (dissoc nb0 :updated-at) (dissoc (sut/get-oldest-neighbour this) :updated-at))
    (sut/delete-neighbour this (:id nb0))
    (match (dissoc nb1 :updated-at) (dissoc (sut/get-oldest-neighbour this) :updated-at))
    (match (dissoc nb3 :updated-at) (dissoc (sut/get-oldest-neighbour this #{:left}) :updated-at))))


;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;
;; SWIM business logic tests
;;;;

(deftest probe-probe-ack-test
  (testing "Probe -> ProbeAck logic"
    (let [node1 (sut/new-node-object node-data1 cluster)
          node2 (sut/new-node-object node-data2 cluster)]
      (try
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

        (testing "After probe neighbour will not be added to neighbours map cause cluster size limit reached"
          (let [before-tx1 (sut/get-tx node1)
                before-tx2 (sut/get-tx node2)]
            (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
            (Thread/sleep 50)
            (match (sut/nodes-in-cluster node1) 1)
            (testing "tx on node 1 is incremented correctly"
              (match (sut/get-tx node1) (+ 2 before-tx1)))  ;; 1 - send probe, 2 - receive ack-probe
            (testing "tx on node 2 is incremented correctly" ;; 1 -receive probe, 2 - send ack-probe
              (match (sut/get-tx node2) (+ 2 before-tx2)))))

        (testing "After probe neighbour will be added to neighbours map cause cluster size limit is not reached"
          (let [before-tx1 (sut/get-tx node1)
                before-tx2 (sut/get-tx node2)]
            (sut/set-cluster-size node1 3)                  ;; increase cluster size
            (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
            (Thread/sleep 20)
            (match (sut/nodes-in-cluster node1) 2)
            (match (:id (sut/get-neighbour node1 (sut/get-id node2))) (:id node-data2))
            (testing "tx on node 1 is incremented correctly"
              (match (sut/get-tx node1) (+ 2 before-tx1)))  ;; 1 - send probe, 2 - receive ack-probe
            (testing "tx on node 2 is incremented correctly" ;; 1 -receive probe, 2 - send ack-probe
              (match (sut/get-tx node2) (+ 2 before-tx2)))))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2))))))


(deftest ack-event-test

  (testing "Don't process event from unknown neighbour"
    (let [node1              (sut/new-node-object node-data1 cluster)
          node2              (sut/new-node-object node-data2 cluster)
          *latest-node-error (atom nil)
          error-catcher-fn   (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (string/ends-with? (str cmd) "error")
                                   (reset! *latest-node-error v))))]
      (try
        (add-tap error-catcher-fn)                          ;; register error catcher
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        ;; sending ack event to node 1 from unknown neighbour
        (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
          (sut/get-host node1) (sut/get-port node1))
        ;; wait for event processing and error-catcher-fn
        (Thread/sleep 25)
        (match (-> *latest-node-error deref :org.rssys.swim/cmd) :ack-event-unknown-neighbour-error)
        (catch Exception e
          (println (ex-message e)))
        (finally
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Don't process event with outdated restart counter"
    (let [node1              (sut/new-node-object node-data1 cluster)
          node2              (sut/new-node-object node-data2 cluster)
          *latest-node-error (atom nil)
          error-catcher-fn   (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (string/ends-with? (str cmd) "error")
                                   (reset! *latest-node-error v))))]
      (try
        (add-tap error-catcher-fn)                          ;; register error catcher
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :restart-counter 999)))

        ;; sending ack event to node 1 from with outdated restart counter
        (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
          (sut/get-host node1) (sut/get-port node1))
        ;; wait for event processing and error-catcher-fn
        (Thread/sleep 25)
        (match (-> *latest-node-error deref :org.rssys.swim/cmd) :ack-event-bad-restart-counter-error)
        (catch Exception e
          (println (ex-message e)))
        (finally
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
          (sut/stop node1)
          (sut/stop node2)))))


  (testing "Don't process event with outdated tx"
    (let [node1              (sut/new-node-object node-data1 cluster)
          node2              (sut/new-node-object node-data2 cluster)
          *latest-node-error (atom nil)
          error-catcher-fn   (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (string/ends-with? (str cmd) "error")
                                   (reset! *latest-node-error v))))]
      (try
        (add-tap error-catcher-fn)                          ;; register error catcher
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :tx 999)))

        ;; sending ack event to node 1 from with outdated tx
        (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
          (sut/get-host node1) (sut/get-port node1))
        ;; wait for event processing and error-catcher-fn
        (Thread/sleep 25)
        (match (-> *latest-node-error deref :org.rssys.swim/cmd) :ack-event-bad-tx-error)
        (catch Exception e
          (println (ex-message e)))
        (finally
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
          (sut/stop node1)
          (sut/stop node2)))))


  (testing "Don't process event from not alive nodes"
    (let [node1              (sut/new-node-object node-data1 cluster)
          node2              (sut/new-node-object node-data2 cluster)
          *latest-node-error (atom nil)
          error-catcher-fn   (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (string/ends-with? (str cmd) "error")
                                   (reset! *latest-node-error v))))]
      (try
        (add-tap error-catcher-fn)                          ;; register error catcher
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :status :dead)))

        ;; sending ack event to node 1 from dead neighbour
        (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
          (sut/get-host node1) (sut/get-port node1))
        ;; wait for event processing and error-catcher-fn
        (Thread/sleep 35)
        (match (-> *latest-node-error deref :org.rssys.swim/cmd) :ack-event-not-alive-neighbour-error)
        (catch Exception e
          (println (ex-message e)))
        (finally
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
          (sut/stop node1)
          (sut/stop node2)))))


  (testing "Don't process event if ack is not requested"
    (let [node1              (sut/new-node-object node-data1 cluster)
          node2              (sut/new-node-object node-data2 cluster)
          *latest-node-error (atom nil)
          error-catcher-fn   (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (string/ends-with? (str cmd) "error")
                                   (reset! *latest-node-error v))))]
      (try
        (add-tap error-catcher-fn)                          ;; register error catcher
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))

        ;; sending ack event to node 1 without ping
        (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
          (sut/get-host node1) (sut/get-port node1))
        ;; wait for event processing and error-catcher-fn
        (Thread/sleep 25)
        (match (-> *latest-node-error deref :org.rssys.swim/cmd) :ack-event-no-active-ping-error)
        (catch Exception e
          (println (ex-message e)))
        (finally
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Process normal ack"
    (let [node1              (sut/new-node-object node-data1 cluster)
          node2              (sut/new-node-object node-data2 cluster)
          *latest-node-error (atom [])
          error-catcher-fn   (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (string/ends-with? (str cmd) "ack-event")
                                   (swap! *latest-node-error conj cmd))))]
      (try
        (add-tap error-catcher-fn)                          ;; register error catcher
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data1))

        (sut/upsert-ping node1 (sut/new-ping-event node1 (sut/get-id node2) 1))
        ;; sending normal ack event to node 1
        (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
          (sut/get-host node1) (sut/get-port node1))
        ;; wait for event processing and error-catcher-fn
        (Thread/sleep 25)
        (match (-> *latest-node-error deref) [:ack-event])
        (catch Exception e
          (println (ex-message e)))
        (finally
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Process normal ack from suspect node"
    (let [node1              (sut/new-node-object node-data1 cluster)
          node2              (sut/new-node-object node-data2 cluster)
          *latest-node-error (atom [])
          error-catcher-fn   (fn [v]
                               (when-let [cmd (:org.rssys.swim/cmd v)]
                                 (when (string/ends-with? (str cmd) "event")
                                   (swap! *latest-node-error conj cmd))))]
      (try
        (add-tap error-catcher-fn)                          ;; register error catcher
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc neighbour-data1 :status :suspect)))

        (sut/upsert-ping node1 (sut/new-ping-event node1 (sut/get-id node2) 1))
        ;; sending normal ack event to node 1 from suspect node
        (sut/send-event node2 (sut/new-ack-event node2 {:id (sut/get-id node1) :tx 0})
          (sut/get-host node1) (sut/get-port node1))
        ;; wait for event processing and error-catcher-fn
        (Thread/sleep 25)
        (match (-> *latest-node-error deref) [:put-event :alive-event :ack-event])
        (match (-> node1 sut/get-outgoing-event-queue first type) AliveEvent) ;; we send new alive event
        (match (count (sut/get-alive-neighbours node1)) 1)  ;; we set new :alive status
        (catch Exception e
          (println (ex-message e)))
        (finally
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
          (sut/stop node1)
          (sut/stop node2))))))


(comment
  (def node1 (sut/new-node-object node-data1 cluster))
  (def node2 (sut/new-node-object node-data2 cluster))
  (sut/start node1 sut/node-process-fn sut/incoming-udp-processor-fn)
  (sut/start node2 sut/node-process-fn sut/incoming-udp-processor-fn)
  (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
  (sut/get-neighbours node1)
  (sut/set-cluster-size node1 3)
  (sut/stop node1)
  (sut/stop node2)
  )






