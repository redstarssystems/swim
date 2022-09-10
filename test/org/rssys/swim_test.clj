(ns org.rssys.swim-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as string]
    [clojure.test :refer [deftest is testing]]
    [matcho.core :refer [match not-match]]
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
      NewClusterSizeEvent
      PingEvent
      ProbeAckEvent
      ProbeEvent)
    (org.rssys.scheduler
      MutablePool)))


(def ^:dynamic *max-test-timeout*
  "Max promise timeout ms in tests"
  1500)


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
      (is (instance? Cluster result) "Should be Cluster instance")
      (is (s/valid? ::spec/cluster result))
      (is (= 3 (.-cluster_size result))))))


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
      (match (sut/get-cluster-size this) 3)
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
      (match (sut/get-cluster-size this) 3)
      (sut/set-cluster-size this 5)
      (match (sut/get-cluster-size this) 5)
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
    (let [this (sut/new-node-object node-data1 (assoc cluster :cluster-size 99))]
      (match (sut/get-neighbours this) empty?)
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
      (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data2))
      (match (sut/get-neighbour this (:id neighbour-data1)) (dissoc neighbour-data1 :updated-at))
      (match (count (sut/get-neighbours this)) 2)

      (testing "don't put to neighbours node with the same id as this have"
        (match (count (sut/get-neighbours this)) 2)
        (sut/upsert-neighbour this neighbour-data3)
        (match (count (sut/get-neighbours this)) 2))

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


(deftest new-alive-event-test
  (let [this        (sut/new-node-object node-data1 cluster)
        ack-event   (event/map->AliveEvent {:cmd-type        3
                                            :id              #uuid "00000000-0000-0000-0000-000000000002"
                                            :restart-counter 5
                                            :tx              0
                                            :neighbour-id    #uuid "00000000-0000-0000-0000-000000000001"
                                            :neighbour-tx    42})
        alive-event (sut/new-alive-event this ack-event)]
    (is (= AliveEvent (type alive-event)) "Should be AliveEvent  type")
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid alive event"
            (sut/new-alive-event this (assoc ack-event :id :bad-value)))))))


(deftest new-cluster-size-event-test
  (let [this      (sut/new-node-object node-data1 cluster)
        ncs-event (sut/new-cluster-size-event this 5)]
    (is (= NewClusterSizeEvent (type ncs-event)) "Should be NewClusterSizeEvent  type")
    (testing "Wrong data is prohibited by spec"
      (is (thrown-with-msg? Exception #"Invalid new cluster size event"
            (sut/new-cluster-size-event this -1))))))


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
    (match (sut/neighbour->vec nb)
      [#uuid "00000000-0000-0000-0000-000000000002"
       "127.0.0.1"
       5377                                                 ;; port
       3                                                    ;; :alive = 3
       0                                                    ;; 0 - direct, 1 - indirect
       3                                                    ;; restart-counter
       0                                                    ;; tx
       {:tcp-port 4567}])))


(deftest vec->neighbour-test
  (testing "Converting NeighbourNode to vector and restore from vector to NeighbourNode is successful"
    (let [nb          (sut/new-neighbour-node neighbour-data1)
          nbv         (sut/neighbour->vec nb)
          restored-nb (sut/vec->neighbour nbv)]
      (match (dissoc restored-nb :updated-at) (dissoc nb :updated-at)))))


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
      (match (sort (sut/build-anti-entropy-data node1))
        [[#uuid "00000000-0000-0000-0000-000000000002"
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
          {}]]))))


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
    (let [this        (sut/new-node-object node-data1 (assoc cluster :cluster-size 999))
          node2       (sut/new-node-object node-data2 (assoc cluster :cluster-size 999))
          probe-event (sut/new-probe-event this (sut/get-host node2) (sut/get-port node2))]
      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start this empty-node-process-fn #(do [%1 %2]))

        (testing "send event using host and port"
          (let [*expecting-event (promise)]
            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))
            (sut/send-event this probe-event (sut/get-host node2) (sut/get-port node2))
            (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
            (match (sut/get-payload node2) [(.prepare probe-event)])
            (remove-watch (:*node node2) :event-detect)))

        (testing "send event using neighbour id"
          (let [*expecting-event (promise)]
            (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))
            (sut/send-event this (event/empty-ack) (sut/get-id node2))
            (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
            (match (sut/get-payload node2) [(.prepare (event/empty-ack))])))

        (testing "Wrong neighbour id is prohibited"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/send-event this (event/empty-ack) (random-uuid)))))

        (testing "Too big UDP packet is prohibited"
          (let [config @sut/*config]
            (swap! sut/*config assoc :max-anti-entropy-items 100)
            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (dotimes [n 100]                          ;; fill too many neighbours
                    (sut/upsert-neighbour this (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))
                  (sut/send-event this (sut/new-anti-entropy-event this) (sut/get-id node2))))
            (reset! sut/*config config)))


        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop this)
          (sut/stop node2))))))


(deftest send-event-ae-test
  (testing "node1 can send event to node2"
    (let [this        (sut/new-node-object node-data1 (assoc cluster :cluster-size 999))
          node2       (sut/new-node-object node-data2 (assoc cluster :cluster-size 999))
          probe-event (sut/new-probe-event this (sut/get-host node2) (sut/get-port node2))]
      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start this empty-node-process-fn #(do [%1 %2]))

        (testing "send event using host and port"
          (let [*expecting-event (promise)]
            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))
            (sut/send-event-ae this probe-event (sut/get-host node2) (sut/get-port node2))
            (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
            (is (= (sut/get-payload node2) [(.prepare probe-event) (.prepare (update (sut/new-anti-entropy-event this) :tx dec))]))))

        (testing "send event using neighbour id"
          (let [*expecting-event (promise)]
            (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))
            (sut/send-event-ae this (event/empty-ack) (sut/get-id node2))
            (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
            (match (sut/get-payload node2) [(.prepare (event/empty-ack)) (.prepare (update (sut/new-anti-entropy-event this) :tx dec))])))

        (testing "Wrong neighbour id is prohibited"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/send-event-ae this (event/empty-ack) (random-uuid)))))

        (testing "Too big UDP packet is prohibited"
          (let [config @sut/*config]
            (swap! sut/*config assoc :max-anti-entropy-items 100)
            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (dotimes [n 100]                          ;; fill too many neighbours
                    (sut/upsert-neighbour this (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))
                  (sut/send-event-ae this (sut/new-anti-entropy-event this) (sut/get-id node2))))
            (reset! sut/*config config)))

        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop this)
          (sut/stop node2))))))


(deftest send-events-test
  (testing "node1 can send all events to node2 with anti-entropy data"
    (let [this        (sut/new-node-object node-data1 (assoc cluster :cluster-size 999))
          node2       (sut/new-node-object node-data2 (assoc cluster :cluster-size 999))
          probe-event (sut/new-probe-event this (sut/get-host node2) (sut/get-port node2))]

      (try
        (sut/start node2 empty-node-process-fn set-payload-incoming-data-processor-fn)
        (sut/start this empty-node-process-fn #(do [%1 %2]))

        (testing "send all events using host and port"
          (let [*expecting-event (promise)]
            (sut/put-event this probe-event)
            (sut/put-event this (event/empty-ack))
            (sut/put-event this (sut/new-anti-entropy-event this))
            (add-watch (:*node node2) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:payload new-val))
                  (deliver *expecting-event (:payload new-val)))))
            (sut/send-events this (sut/take-events this) (sut/get-host node2) (sut/get-port node2))
            (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
            (match (sut/get-payload node2)
              [(.prepare probe-event) (.prepare (event/empty-ack)) (.prepare (sut/new-anti-entropy-event this))])
            (is (empty? (sut/get-outgoing-event-queue this)) "After send outgoing queue should be empty")))

        ;; ************ this is for ping tests
        #_(testing "send all events using neighbour id"
            (let [*expecting-event (promise)]
              (sut/put-event this probe-event)
              (sut/put-event this (event/empty-ack))
              (sut/upsert-neighbour this (sut/new-neighbour-node neighbour-data1))
              (add-watch (:*node node2) :event-detect
                (fn [_ _ _ new-val]
                  (when-not (empty? (:payload new-val))
                    (deliver *expecting-event (:payload new-val)))))
              (sut/send-events this (sut/get-id node2))
              (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
              (match (sut/get-payload node2)
                [(.prepare probe-event) (.prepare (event/empty-ack)) (.prepare (sut/new-anti-entropy-event this))])
              (is (empty? (sut/get-outgoing-event-queue this)) "After send outgoing queue should be empty")))

        #_(testing "Wrong neighbour id is prohibited"
            (is (thrown-with-msg? Exception #"Unknown neighbour id"
                  (sut/put-event this probe-event)
                  (sut/send-events this (random-uuid)))))

        (testing "Too big UDP packet is prohibited"
          (let [config @sut/*config]                        ;; increase from 2 to 100
            (swap! sut/*config assoc :max-anti-entropy-items 100)
            (is (thrown-with-msg? Exception #"UDP packet is too big"
                  (dotimes [n 100]                          ;; fill too many neighbours
                    (sut/upsert-neighbour this (sut/new-neighbour-node (random-uuid) "127.0.0.1" (inc (rand-int 10240)))))
                  (sut/send-events this [(sut/new-anti-entropy-event this)] (sut/get-host node2) (sut/get-port node2))))
            (reset! sut/*config config)))

        (catch Exception e
          (println (.getMessage e)))
        (finally
          (sut/stop this)
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
    (match (dissoc nb0 :updated-at) (dissoc (sut/get-oldest-neighbour this) :updated-at))
    (sut/delete-neighbour this (:id nb0))
    (match (dissoc nb11 :updated-at) (dissoc (sut/get-oldest-neighbour this) :updated-at))
    (match (dissoc nb3 :updated-at) (dissoc (sut/get-oldest-neighbour this #{:left}) :updated-at))))


(deftest cluster-size-exceed?-test
  (let [this (sut/new-node-object node-data1 (assoc cluster :cluster-size 1))]
    (is (true? (sut/cluster-size-exceed? this)))
    (sut/set-cluster-size this 3)
    (is (false? (sut/cluster-size-exceed? this)))))


;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;
;; SWIM business logic tests
;;;;

(deftest ^:logic probe-probe-ack-test
  (testing "Probe -> ProbeAck logic"
    (let [node1 (sut/new-node-object node-data1 (assoc cluster :cluster-size 1))
          node2 (sut/new-node-object node-data2 (assoc cluster :cluster-size 1))]
      (try
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)

        (testing "After probe neighbour will not be added to neighbours map cause cluster size limit reached"
          (let [before-tx1       (sut/get-tx node1)
                before-tx2       (sut/get-tx node2)
                *expecting-event (promise)]
            (add-watch (:*node node1) :event-detect
              (fn [_ _ _ new-val]
                (when (= 3 (:tx new-val))                   ;; wait for ack-probe from node2
                  (deliver *expecting-event (:payload new-val)))))
            (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
            (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
            (match (sut/nodes-in-cluster node1) 1)
            (testing "tx on node 1 is incremented correctly"
              (match (sut/get-tx node1) (+ 2 before-tx1)))  ;; 1 - send probe, 2 - receive ack-probe
            (testing "tx on node 2 is incremented correctly" ;; 1 -receive probe, 2 - send ack-probe
              (match (sut/get-tx node2) (+ 2 before-tx2)))))

        (testing "After probe neighbour will be added to neighbours map cause cluster size limit is not reached"
          (let [before-tx1       (sut/get-tx node1)
                before-tx2       (sut/get-tx node2)
                *expecting-event (promise)]
            (sut/set-cluster-size node1 3)                  ;; increase cluster size
            (add-watch (:*node node1) :event-detect
              (fn [_ _ _ new-val]
                (when-not (empty? (:neighbours new-val))
                  (deliver *expecting-event 1))))
            (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
            (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
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
          (sut/stop node2))))

    (testing "Do not add neighbour to neighbours map if status is :alive or :suspect"
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
          (match (sut/nodes-in-cluster node1) 1)
          ;; change status to any :alive or :suspect to verify that neighbour will not added
          (sut/set-status node1 :alive)
          (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
          (sut/set-status node1 :suspect)
          (sut/probe node1 (sut/get-host node2) (sut/get-port node2))
          (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
          (match (sut/nodes-in-cluster node1) 1)
          (catch Exception e
            (println (ex-message e)))
          (finally
            (remove-tap event-catcher-fn)
            (sut/stop node1)
            (sut/stop node2)))))))


(deftest ^:logic ack-event-test

  (testing "Don't process ack event from unknown neighbour"
    (let [node1            (sut/new-node-object node-data1 cluster)
          node2            (sut/new-node-object node-data2 cluster)
          *expecting-error (promise)
          error-catcher-fn (fn [v]
                             (when-let [cmd (:org.rssys.swim/cmd v)]
                               (when (= cmd :ack-event-unknown-neighbour-error)
                                 (deliver *expecting-error cmd))))]
      (try
        (add-tap error-catcher-fn)                          ;; register error catcher
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
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
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
        (add-tap error-catcher-fn)                          ;; register error catcher
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
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
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
        (add-tap error-catcher-fn)                          ;; register error catcher
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
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
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
        (add-tap error-catcher-fn)                          ;; register error catcher
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
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
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
        (add-tap error-catcher-fn)                          ;; register error catcher
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
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
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
        (add-tap event-catcher-fn)                          ;; register event catcher
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
          (remove-tap event-catcher-fn)                     ;; unregister event catcher
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
        (add-tap event-catcher-fn)                          ;; register event catcher
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
        (match (-> node1 sut/get-outgoing-event-queue first type) AliveEvent) ;; we send new alive event
        (match (count (sut/get-alive-neighbours node1)) 1)  ;; we set new :alive status
        (catch Exception e
          (println (ex-message e)))
        (finally
          (remove-tap event-catcher-fn)                     ;; unregister event catcher
          (sut/stop node1)
          (sut/stop node2))))))


(deftest ^:logic anti-entropy-test

  (testing "normal anti-entropy data from node1 is accepted on node2"
    (let [node1            (sut/new-node-object node-data1 cluster)
          node2            (sut/new-node-object node-data2 cluster)
          *expecting-event (promise)]
      (try

        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node neighbour-data3))
        (add-watch (:*node node2) :event-detect
          (fn [_ _ _ new-val]
            (when (= 2 (count (:neighbours new-val)))       ;; wait for anti-entropy on node2
              (deliver *expecting-event (:neighbours new-val)))))
        (match (count (sut/get-neighbours node2)) 1)
        ;; sending normal anti-entropy event to node 2
        (sut/inc-tx node1)
        (sut/send-event node1 (sut/new-anti-entropy-event node1) (sut/get-host node2) (sut/get-port node2))
        ;; wait for event processing and event-catcher-fn
        (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
        (is (= 2 (count (sut/get-neighbours node2))))
        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "anti-entropy with actual incarnation is accepted on node2"
    (let [node1            (sut/new-node-object node-data1 cluster)
          node2            (sut/new-node-object node-data2 cluster)
          *expecting-event (promise)]
      (try
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc neighbour-data2 :payload {})))
        (add-watch (:*node node2) :event-detect
          (fn [_ _ _ new-val]
            (when (= 1 (count (:neighbours new-val)))       ;; wait for anti-entropy on node2
              (deliver *expecting-event (:neighbours new-val)))))

        ;; sending normal anti-entropy event to node 2
        (sut/send-event node1 (sut/new-anti-entropy-event node1) (sut/get-host node2) (sut/get-port node2))
        ;; wait for event processing and event-catcher-fn
        (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
        (match (= (:payload neighbour-data2) (:payload (sut/get-neighbour node2 (:id neighbour-data2)))))
        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "anti-entropy with outdated incarnation is denied on node2"
    (let [node1            (sut/new-node-object node-data1 cluster)
          node2            (sut/new-node-object node-data2 cluster)
          *expecting-event (promise)]
      (try
        (sut/start node1 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/start node2 empty-node-process-fn sut/incoming-udp-processor-fn)
        (sut/upsert-neighbour node1 (sut/new-neighbour-node neighbour-data2))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc neighbour-data2 :payload {} :restart-counter 999)))
        (add-watch (:*node node2) :event-detect
          (fn [_ _ _ new-val]
            (when (= 1 (count (:neighbours new-val)))       ;; wait for anti-entropy on node2
              (deliver *expecting-event (:neighbours new-val)))))
        ;; sending normal anti-entropy event to node 2
        (sut/send-event node1 (sut/new-anti-entropy-event node1) (sut/get-host node2) (sut/get-port node2))
        ;; wait for event processing and event-catcher-fn
        (not-match (deref *expecting-event *max-test-timeout* :timeout) :timeout)
        (match (= {} (:payload (sut/get-neighbour node2 (:id neighbour-data2)))))
        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2))))))


(deftest ^:logic ping-event-test

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
        (add-tap error-catcher-fn)                      ;; register error catcher
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
          (remove-tap error-catcher-fn)                 ;; unregister error catcher
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
        (add-tap error-catcher-fn)                  ;; register error catcher
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
          (remove-tap error-catcher-fn)             ;; unregister error catcher
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
        (add-tap error-catcher-fn)                          ;; register error catcher
        (add-tap error-catcher-fn2)                         ;; register error catcher
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
          (remove-tap error-catcher-fn)                     ;; unregister error catcher
          (remove-tap error-catcher-fn2)                    ;; unregister error catcher
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
        (add-tap error-catcher-fn)                  ;; register error catcher
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
          (remove-tap error-catcher-fn)             ;; unregister error catcher
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
        (add-tap error-catcher-fn)                  ;; register error catcher
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
          (remove-tap error-catcher-fn)             ;; unregister error catcher
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
        (add-tap event-catcher-fn)                  ;; register event catcher
        (add-tap event-catcher-fn2)                 ;; register event catcher
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
          (remove-tap event-catcher-fn)             ;; unregister event catcher
          (remove-tap event-catcher-fn2)            ;; unregister event catcher
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
        (add-tap event-catcher-fn)                  ;; register event catcher
        (add-tap event-catcher-fn2)                 ;; register event catcher
        (add-tap event-catcher-fn3)                 ;; register event catcher
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
          (remove-tap event-catcher-fn)             ;; unregister event catcher
          (remove-tap event-catcher-fn2)            ;; unregister event catcher
          (remove-tap event-catcher-fn3)            ;; unregister event catcher
          (sut/stop node1)
          (sut/stop node2))))))



