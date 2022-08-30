(ns org.rssys.swim-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test :refer [deftest is testing]]
    [matcho.core :refer [match]]
    [org.rssys.swim :as sut])
  (:import
    (org.rssys.scheduler
      MutablePool)
    (org.rssys.swim
      AckEvent
      AntiEntropy
      Cluster
      DeadEvent
      NeighbourNode
      NodeObject
      PingEvent
      ProbeEvent)))


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


;;;;;;;;;;


(deftest new-cluster-test
  (testing "Create Cluster instance is successful"
    (let [result (sut/new-cluster cluster-data)]
      (is (instance? Cluster result) "Should be Cluster instance")
      (is (s/valid? ::sut/cluster result))
      (is (= 1 (.-cluster_size result))))))


(deftest new-neighbour-node-test
  (testing "Create NeighbourNode instance is successful"
    (let [result1 (sut/new-neighbour-node neighbour-data1)
          result2 (sut/new-neighbour-node #uuid "00000000-0000-0000-0000-000000000000" "127.0.0.1" 5379)]
      (is (instance? NeighbourNode result1) "Should be NeighbourNode instance")
      (is (instance? NeighbourNode result2) "Should be NeighbourNode instance")
      (is (s/valid? ::sut/neighbour-node result1))
      (is (s/valid? ::sut/neighbour-node result2))
      (match result1 neighbour-data1)
      (match result2 {:id              #uuid "00000000-0000-0000-0000-000000000000"
                      :host            "127.0.0.1"
                      :port            5379
                      :status          :unknown
                      :access          :direct
                      :restart-counter 0
                      :tx              0
                      :payload         {}
                      :updated-at      nat-int?}))))


(def cluster (sut/new-cluster cluster-data))


(deftest new-node-object-test

  (testing "Create NodeObject instance is successful"
    (let [result1 (sut/new-node-object {:id #uuid "00000000-0000-0000-0000-000000000001" :host "127.0.0.1" :port 5376} cluster)]
      (is (instance? NodeObject result1) "Should be NodeObject instance")
      (is (s/valid? ::sut/node (.value result1)))
      (match (.value result1)
        {:id                #uuid "00000000-0000-0000-0000-000000000001"
         :host              "127.0.0.1"
         :port              5376
         :cluster           cluster
         :status            :stop
         :neighbours        {}
         :restart-counter   0
         :tx                0
         :ping-events       {}
         :payload           {}
         :event-queue       []
         :ping-round-buffer []
         :scheduler-pool    #(instance? MutablePool %)
         :*udp-server       nil})))

  (testing "Wrong data is caught by spec"
    (is (thrown-with-msg? Exception #"Invalid node data"
          (sut/new-node-object {:a 1} cluster)))))


(deftest map->NodeObject-test

  (testing "Getters should return expected values"
    (let [node-object (sut/new-node-object node-data1 cluster)
          result      (.value node-object)]

      (match result {:id                ::sut/id
                     :host              ::sut/host
                     :port              ::sut/port
                     :cluster           ::sut/cluster
                     :status            ::sut/status
                     :neighbours        ::sut/neighbours
                     :restart-counter   ::sut/restart-counter
                     :tx                ::sut/tx
                     :ping-events       ::sut/ping-events
                     :payload           ::sut/payload
                     :scheduler-pool    ::sut/scheduler-pool
                     :*udp-server       ::sut/*udp-server
                     :event-queue       ::sut/event-queue
                     :ping-round-buffer ::sut/ping-round-buffer})

      ;; Tests for getters
      (is (= #uuid "00000000-0000-0000-0000-000000000001" (.id node-object)) "Should be UUID value")
      (is (= "127.0.0.1" (.host node-object)))
      (is (= 5376 (.port node-object)))
      (is (= cluster (.cluster node-object)) "Should be Cluster value")
      (is (= 1 (.cluster_size node-object)) "Should be Cluster size")
      (is (= 7 (.restart_counter node-object)) "Should be restart counter value")
      (is (= 0 (.tx node-object)) "Should be tx value")
      (is (= {} (.payload node-object)) "Should be payload value")
      (is (= {} (.neighbours node-object)) "Should be neighbours value")
      (is (= :stop (.status node-object)) "Should be a node status value")
      (is (= [] (.event_queue node-object)) "Event queue should be a vector")
      (is (= {} (.ping_events node-object)) "Ping events should be a map")))

  (testing "Setters should set correct values"

    (testing "Correct cluster value should set successfully"
      (let [new-cluster (sut/new-cluster (assoc cluster-data :id (random-uuid) :name "cluster2"))
            node-object (sut/new-node-object node-data1 cluster)]
        (is (= cluster (.cluster node-object)) "Node has current cluster value")
        (.set_cluster node-object new-cluster)
        (is (= new-cluster (.cluster node-object)) "Node has new cluster value")
        (.set_cluster_size node-object 42)
        (is (= (:cluster-size (.cluster node-object)) 42) "Cluster size should have new value")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid cluster data"
                (.set_cluster node-object (assoc cluster :id 1)))))

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid cluster size"
                (.set_cluster_size node-object :bad-value))))

        (testing "Cluster change allowed only in stopped status"
          (is (thrown-with-msg? Exception #"Node is not stopped"
                (swap! (:*node node-object) assoc :status :left)
                (.set_cluster node-object new-cluster))))))

    (testing "Increment tx should be successful"
      (let [node-object (sut/new-node-object node-data1 cluster)]
        (is (= 0 (.tx node-object)) "Node has current (default) value")
        (.inc_tx node-object)
        (is (= 1 (.tx node-object)) "Node has expected tx value")))

    (testing "Correct payload value should set successfully"
      (let [new-payload {:tcp-port 1234 :role "data node"}
            node-object (sut/new-node-object node-data1 cluster)]
        (is (= {} (.payload node-object)) "Node has current (default) payload value")
        (.set_payload node-object new-payload)
        (is (= new-payload (.payload node-object)) "Node has new payload value")))

    (testing "Correct node status should set successfully"
      (let [new-status  :left
            node-object (sut/new-node-object node-data1 cluster)]
        (is (= :stop (.status node-object)) "Node has stop status")
        (.set_status node-object new-status)
        (is (= new-status (.status node-object)) "Node has new status")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid node status"
                (.set_status node-object :wrong-value))))))

    (testing "Correct restart counter value should set successfully"
      (let [new-restart-counter 123
            node-object         (sut/new-node-object node-data1 cluster)]
        (is (= 7 (:restart-counter (.value node-object))) "Node has current restart counter value")
        (.set_restart_counter node-object new-restart-counter)
        (is (= new-restart-counter (:restart-counter (.value node-object))) "Node has new restart counter value")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid restart counter data"
                (.set_restart_counter node-object :wrong-value))))))

    (testing "Correct neighbour node value should set successfully"
      (let [neighbour-node (sut/new-neighbour-node neighbour-data1)
            node-object    (sut/new-node-object node-data1 cluster)]
        (is (= {} (.neighbours node-object)) "Node has current (default) neighbours value")
        (.upsert_neighbour node-object neighbour-node)
        (is (= (-> neighbour-node (dissoc :updated-at)) (-> node-object .neighbours (get (:id neighbour-node)) (dissoc :updated-at))) "Node has new neighbours value")
        (is (= (-> neighbour-node (dissoc :updated-at)) (-> node-object (.neighbour (:id neighbour-node)) (dissoc :updated-at))) "Neighbour getter by id works")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid neighbour node data"
                (.upsert_neighbour node-object {:a :bad-value}))))))

    (testing "Set timestamp after every neighbour update"
      (let [neighbour-node (sut/new-neighbour-node neighbour-data1)
            node-object    (sut/new-node-object node-data1 cluster)
            _              (.upsert_neighbour node-object neighbour-node)
            t1             (:updated-at (get (.neighbours node-object) (.-id neighbour-node)))
            _              (Thread/sleep 1)
            _              (.upsert_neighbour node-object neighbour-node)
            t2             (:updated-at (get (.neighbours node-object) (.-id neighbour-node)))]
        (is (> t2 t1) "Timestamp should be updated")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid neighbour node data"
                (.upsert_neighbour node-object {:a :bad-value}))))))

    (testing "Neighbour node is deleted successfully successfully"
      (let [neighbour-node1 (sut/new-neighbour-node neighbour-data1)
            neighbour-node2 (sut/new-neighbour-node neighbour-data2)
            neighbour-node3 (sut/new-neighbour-node neighbour-data3)
            node-object     (sut/new-node-object node-data1 cluster)]
        (.upsert_neighbour node-object neighbour-node1)
        (.upsert_neighbour node-object neighbour-node2)
        (.upsert_neighbour node-object neighbour-node3)
        (is (= (-> neighbour-node1 (dissoc :updated-at)) (-> node-object .neighbours (get (:id neighbour-node1)) (dissoc :updated-at))) "Neighbour1 is present")

        (.delete_neighbour node-object (:id neighbour-node1))
        (is (= (keys (.neighbours node-object)) (map :id [neighbour-node2 neighbour-node3]))
          "Neighbour1 should not present")))

    (testing "Correct event queue value should set successfully"
      (let [new-event-queue [[(:left sut/event-code) (random-uuid)]]
            node-object     (sut/new-node-object node-data1 cluster)]
        (is (= [] (.event_queue node-object)) "Event queue has current (default) value")
        (.set_event_queue node-object new-event-queue)
        (is (= new-event-queue (.event_queue node-object)) "Node has new event queue value")))

    (testing "Put event to queue is successful"
      (let [prepared-left-event [(:left sut/event-code) (random-uuid)]
            node-object         (sut/new-node-object node-data1 cluster)]
        (is (= [] (.event_queue node-object)) "Event queue has current (default) value")
        (.put_event node-object prepared-left-event)
        (is (= [prepared-left-event] (.event_queue node-object)) "Node has new event queue value")
        (.put_event node-object [2])
        (.put_event node-object [3])
        (.put_event node-object [4])
        (is (= 4 (.tx node-object)) "Every put event on node should increase tx value")))

    (testing "Take event from queue is successful"
      (let [prepared-left-event [(:left sut/event-code) (random-uuid)]
            node-object         (sut/new-node-object node-data1 cluster)]
        (.put_event node-object prepared-left-event)
        (is (= prepared-left-event (.take_event node-object)) "Take event got expected value from queue")
        (is (= [] (.event_queue node-object)) "Event queue should be empty")))

    (testing "Take events from queue is successful"
      (let [node-object (sut/new-node-object node-data1 cluster)]
        (.put_event node-object [1])
        (.put_event node-object [2])
        (.put_event node-object [3])
        (.put_event node-object [4])
        (is (= [[1] [2]] (.take_events node-object 2)) "Take events got expected values from queue")
        (is (= [[3] [4]] (.event_queue node-object)) "Event queue should have expected values")))

    (testing "Ping event getters/setters test"
      (let [neighbour-node (sut/new-neighbour-node neighbour-data1)
            node-object    (sut/new-node-object node-data1 cluster)
            neighbour-id   (.-id neighbour-node)
            ping-event     (sut/new-ping node-object neighbour-id 1)]
        (is (= {} (.ping_events node-object)) "Node has empty (default) ping events map")
        (.upsert_ping node-object ping-event)
        (is (= ping-event (.ping_event node-object neighbour-id)) "Ping event should exist in map")
        (is (= {neighbour-id ping-event} (.ping_events node-object)) "Ping map has new ping event")
        (.delete_ping node-object neighbour-id)
        (is (= {} (.ping_events node-object)) "Ping event should be deleted from map")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid ping event data"
                (.upsert_ping node-object {:a :bad-value}))))))))


;;;;;;;;;;

(deftest calc-n-test
  (testing "How many nodes should we notify depending on N nodes in a cluster"
    (let [nodes-in-cluster [1 2 4 8 16 32 64 128 256 512 1024]
          result           (mapv sut/calc-n nodes-in-cluster)]
      (is (= [0 1 2 3 4 5 6 7 8 9 10] result)))))


;;;;;;;;;;


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

(deftest new-ping-test

  (testing "PingEvent creation"
    (let [node1  (sut/new-node-object node-data1 cluster)
          node2  (sut/new-node-object node-data2 cluster)
          result (sut/new-ping node1 (.id node2) 1)]

      (is (= PingEvent (type result)) "PingEvent has correct type")

      (is (= #{:cmd-type :id :host :port :restart-counter :tx :neighbour-id :attempt-number}
            (into #{} (keys result))) "PingEvent has expected keys")

      (testing "PingEvent has correct structure and expected values"
        (match result {:cmd-type        (:ping sut/event-code)
                       :id              (.id node1)
                       :host            (.host node1)
                       :port            (.port node1)
                       :restart-counter (.restart_counter node1)
                       :tx              (.tx node1)
                       :neighbour-id    (.id node2)
                       :attempt-number  1})))))


(deftest empty-ping-test

  (testing "Empty PingEvent has correct structure"
    (let [result (sut/empty-ping)]

      (is (= PingEvent (type result)) "PingEvent has correct type")

      (match result {:cmd-type        (:ping sut/event-code)
                     :id              #uuid"00000000-0000-0000-0000-000000000000"
                     :host            "localhost"
                     :port            0
                     :restart-counter 0
                     :tx              0
                     :neighbour-id    #uuid"00000000-0000-0000-0000-000000000000"
                     :attempt-number  1}))))


(deftest map->PingEvent-test

  (testing "PingEvent"
    (let [node1  (sut/new-node-object node-data1 cluster)
          ping1  (sut/new-ping node1 #uuid "8acc376e-f90d-470b-aa58-400a339d9424" 42)
          result (.prepare ping1)]

      (testing "Prepare PingEvent to vector"
        (match result [(:ping sut/event-code) (.-id ping1) (.-host ping1) (.-port ping1) (.-restart_counter ping1) (.-tx ping1) (.-neighbour_id ping1) (.-attempt_number ping1)]))

      (testing "Restore PingEvent from vector"

        (let [v           [0
                           #uuid "00000000-0000-0000-0000-000000000001"
                           "127.0.0.1"
                           5376
                           7
                           0
                           #uuid "8acc376e-f90d-470b-aa58-400a339d9424"
                           42]
              result-ping (.restore (sut/empty-ping) v)]

          (is (= PingEvent (type result-ping)) "Should be PingEvent type")

          (is (= result-ping ping1) "Restored PingEvent should be equals to original event")

          (is (thrown-with-msg? Exception #"PingEvent vector has invalid structure"
                (.restore (sut/empty-ping) [])))

          (testing "Wrong command type code"
            (is (thrown-with-msg? Exception #"PingEvent vector has invalid structure"
                  (.restore (sut/empty-ping) [999
                                              #uuid "00000000-0000-0000-0000-000000000001"
                                              7
                                              0
                                              #uuid "8b8d59ca-f1c5-4c9e-a4db-6d09bfb2751c"
                                              1])))))))))


;;;;;;;;;;


(deftest new-ack-test
  (testing "AckEvent creation"
    (let [node1  (sut/new-node-object node-data1 cluster)
          node2  (sut/new-node-object node-data2 cluster)
          ping1  (sut/new-ping node1 (.id node2) 42)
          result (sut/new-ack node2 ping1)]

      (is (= AckEvent (type result)) "PingEvent has correct type")

      (is (= #{:cmd-type :id :restart-counter :tx :neighbour-id :neighbour-tx}
            (into #{} (keys result))) "AckEvent has expected keys")

      (testing "AckEvent has correct structure and values"
        (match result {:cmd-type        (:ack sut/event-code)
                       :id              (.id node2)
                       :restart-counter (.restart_counter node2)
                       :tx              (.tx node2)
                       :neighbour-id    (.id node1)
                       :neighbour-tx    (.tx node1)})))))


(deftest empty-ack-test

  (testing "Empty AckEvent has correct structure"
    (let [result (sut/empty-ack)]

      (is (= AckEvent (type result)) "AckEvent has correct type")

      (match result {:cmd-type        (:ack sut/event-code)
                     :id              #uuid"00000000-0000-0000-0000-000000000000"
                     :restart-counter 0
                     :tx              0
                     :neighbour-id    #uuid"00000000-0000-0000-0000-000000000000"
                     :neighbour-tx    0}))))


(deftest map->AckEvent-test

  (testing "AckEvent"
    (let [node1  (sut/new-node-object node-data1 cluster)
          node2  (sut/new-node-object node-data2 cluster)
          ping1  (sut/new-ping node1 (.id node2) 42)
          ack1   (sut/new-ack node2 ping1)
          result (.prepare ack1)]

      (testing "Prepare AckEvent to vector"
        (match result [(:ack sut/event-code) (.id node2) (.restart_counter node2) (.tx node2) (.id node1) (.tx node1)]))

      (testing "Restore AckEvent from vector"

        (let [v          [1
                          #uuid "00000000-0000-0000-0000-000000000002"
                          5
                          0
                          #uuid "00000000-0000-0000-0000-000000000001"
                          0]
              result-ack (.restore (sut/empty-ack) v)]

          (is (= AckEvent (type result-ack)) "Should be AckEvent type")

          (is (= result-ack ack1) "Restored AckEvent should be equals to original event")

          (is (thrown-with-msg? Exception #"AckEvent vector has invalid structure"
                (.restore (sut/empty-ack) [])))

          (testing "Wrong command type code"
            (is (thrown-with-msg? Exception #"AckEvent vector has invalid structure"
                  (.restore (sut/empty-ack) [999
                                             #uuid "00000000-0000-0000-0000-000000000002"
                                             5
                                             0
                                             #uuid "00000000-0000-0000-0000-000000000001"
                                             1])))))))))


;;;;;;

(deftest new-dead-test
  (testing "DeadEvent creation"
    (let [node1  (sut/new-node-object node-data1 cluster)
          node2  (sut/new-node-object node-data2 cluster)
          ping1  (sut/new-ping node1 (.id node2) 42)
          result (sut/new-dead node2 ping1)]

      (is (= DeadEvent (type result)) "DeadEvent has correct type")

      (is (= #{:cmd-type :id :restart-counter :tx :neighbour-id :neighbour-tx}
            (into #{} (keys result))) "DeadEvent has expected keys")

      (testing "DeadEvent has correct structure and values"
        (match result {:cmd-type        (:dead sut/event-code)
                       :id              (.id node2)
                       :restart-counter (.restart_counter node2)
                       :tx              (.tx node2)
                       :neighbour-id    (.id node1)
                       :neighbour-tx    (.tx node1)})))))


(deftest empty-dead-test

  (testing "Empty DeadEvent has correct structure"
    (let [result (sut/empty-dead)]

      (is (= DeadEvent (type result)) "DeadEvent has correct type")

      (match result {:cmd-type        (:dead sut/event-code)
                     :id              #uuid"00000000-0000-0000-0000-000000000000"
                     :restart-counter 0
                     :tx              0
                     :neighbour-id    #uuid"00000000-0000-0000-0000-000000000000"
                     :neighbour-tx    0}))))


(deftest map->DeadEvent-test

  (testing "DeadEvent"
    (let [node1  (sut/new-node-object node-data1 cluster)
          node2  (sut/new-node-object node-data2 cluster)
          ping1  (sut/new-ping node1 (.id node2) 42)
          dead1  (sut/new-dead node2 ping1)
          result (.prepare dead1)]

      (testing "Prepare DeadEvent to vector"
        (match result [(:dead sut/event-code) (.id node2) (.restart_counter node2) (.tx node2) (.id node1) (.tx node1)]))

      (testing "Restore DeadEvent from vector"

        (let [v           [6
                           #uuid "00000000-0000-0000-0000-000000000002"
                           5
                           0
                           #uuid "00000000-0000-0000-0000-000000000001"
                           0]
              result-dead (.restore (sut/empty-dead) v)]

          (is (= DeadEvent (type result-dead)) "Should be DeadEvent type")

          (is (= result-dead dead1) "Restored DeadEvent should be equals to original event")

          (is (thrown-with-msg? Exception #"DeadEvent vector has invalid structure"
                (.restore (sut/empty-dead) [])))

          (testing "Wrong command type code"
            (is (thrown-with-msg? Exception #"DeadEvent vector has invalid structure"
                  (.restore (sut/empty-dead) [999
                                              #uuid "00000000-0000-0000-0000-000000000002"
                                              5
                                              0
                                              #uuid "00000000-0000-0000-0000-000000000001"
                                              1])))))))))



;;;;;;;;;;


(deftest suitable-restart-counter?-test
  (let [node1 (sut/new-node-object node-data1 cluster)
        node2 (sut/new-node-object node-data2 cluster)
        ping1 (sut/new-ping node1 (.id node2) 42)]


    (testing "Normal restart counter from ping should be accepted"
      ;; add new normal neighbour in map
      (.upsert_neighbour node2 (sut/new-neighbour-node {:id              (.-id ping1)
                                                        :host            (.-host ping1)
                                                        :port            (.-port ping1)
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter (.-restart_counter ping1)
                                                        :tx              (.-tx ping1)
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))

      (is (true? (sut/suitable-restart-counter? node2 ping1))))


    (testing "Restart counter from ping should be denied"
      ;; add neighbour with restart counter more than ping has
      (.upsert_neighbour node2 (sut/new-neighbour-node {:id              (.-id ping1)
                                                        :host            (.-host ping1)
                                                        :port            (.-port ping1)
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter (inc (.-restart_counter ping1)) ;;
                                                        :tx              (.-tx ping1)
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))

      (is (not (sut/suitable-restart-counter? node2 ping1))))

    (testing "Nil or absent value for neighbour id will not crash"
      (is (not (sut/suitable-restart-counter? node2 nil)))
      (is (not (sut/suitable-restart-counter? node2 {:id 123}))))))


(deftest suitable-tx?-test
  (let [node1 (sut/new-node-object node-data1 cluster)
        node2 (sut/new-node-object node-data2 cluster)
        ping1 (sut/new-ping node1 (.id node2) 42)]


    (testing "Normal tx from ping should be accepted"
      ;; add new normal neighbour in map
      (.upsert_neighbour node2 (sut/new-neighbour-node {:id              (.-id ping1)
                                                        :host            (.-host ping1)
                                                        :port            (.-port ping1)
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter (.-restart_counter ping1)
                                                        :tx              (.-tx ping1)
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))

      (is (true? (sut/suitable-tx? node2 ping1))))


    (testing "tx from ping should be denied"
      ;; add neighbour with tx more than ping has
      (.upsert_neighbour node2 (sut/new-neighbour-node {:id              (.-id ping1)
                                                        :host            (.-host ping1)
                                                        :port            (.-port ping1)
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter (.-restart_counter ping1) ;;
                                                        :tx              (inc (.-tx ping1))
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))

      (is (not (sut/suitable-tx? node2 ping1))))

    (testing "Nil or absent value for neighbour id will not crash"
      (is (not (sut/suitable-tx? node2 nil)))
      (is (not (sut/suitable-tx? node2 {:id 123}))))))


(deftest suitable-incarnation?-test
  (let [node1 (sut/new-node-object node-data1 cluster)
        node2 (sut/new-node-object node-data2 cluster)
        ping1 (sut/new-ping node1 (.id node2) 42)]

    (testing "ping from normal neighbour should be accepted"
      ;; add new normal neighbour in map
      (.upsert_neighbour node2 (sut/new-neighbour-node {:id              (.-id ping1)
                                                        :host            (.-host ping1)
                                                        :port            (.-port ping1)
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter (.-restart_counter ping1)
                                                        :tx              (.-tx ping1)
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))

      (is (true? (sut/suitable-incarnation? node2 ping1))))


    (testing "ping with older tx should be denied"
      ;; add neighbour with tx more than ping has
      (.upsert_neighbour node2 (sut/new-neighbour-node {:id              (.-id ping1)
                                                        :host            (.-host ping1)
                                                        :port            (.-port ping1)
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter (.-restart_counter ping1) ;;
                                                        :tx              (inc (.-tx ping1))
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))

      (is (not (sut/suitable-incarnation? node2 ping1))))

    (testing "ping with older restart counter should be denied"
      ;; add neighbour with tx more than ping has
      (.upsert_neighbour node2 (sut/new-neighbour-node {:id              (.-id ping1)
                                                        :host            (.-host ping1)
                                                        :port            (.-port ping1)
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter (inc (.-restart_counter ping1)) ;;
                                                        :tx              (.-tx ping1)
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))

      (is (not (sut/suitable-incarnation? node2 ping1))))

    (testing "Nil or absent value for neighbour id will not crash"
      (is (not (sut/suitable-incarnation? node2 nil)))
      (is (not (sut/suitable-incarnation? node2 {:id 123}))))))


(deftest safe-test
  (is (= nil (sut/safe (/ 1 0))) "Any Exceptions should be prevented")
  (is (= 1/2 (sut/safe (/ 1 2)))) "Any normal expression should be succeed")



;;;;;;;;;;

(deftest new-probe-test

  (testing "ProbeEvent creation"
    (let [node1  (sut/new-node-object node-data1 cluster)
          node2  (sut/new-node-object node-data2 cluster)
          result (sut/new-probe node1 (.host node2) (.port node2))]

      (is (= ProbeEvent (type result)) "ProbeEvent has correct type")

      (is (= #{:cmd-type :id :host :port :restart-counter :tx :neighbour-host :neighbour-port}
            (into #{} (keys result))) "ProbeEvent has expected keys")

      (testing "ProbeEvent has correct structure and expected values"
        (match result {:cmd-type        (:probe sut/event-code)
                       :id              (.id node1)
                       :host            (.host node1)
                       :port            (.port node1)
                       :restart-counter (.restart_counter node1)
                       :tx              (.tx node1)
                       :neighbour-host  (.host node2)
                       :neighbour-port  (.port node2)})))))


(deftest empty-probe-test

  (testing "Empty ProbeEvent has correct structure"
    (let [result (sut/empty-probe)]

      (is (= ProbeEvent (type result)) "ProbeEvent has correct type")

      (match result {:cmd-type        (:probe sut/event-code)
                     :id              #uuid"00000000-0000-0000-0000-000000000000"
                     :host            "localhost"
                     :port            0
                     :restart-counter 0
                     :tx              0
                     :neighbour-host  "localhost"
                     :neighbour-port  0}))))


(deftest map->ProbeEvent-test

  (testing "ProbeEvent"
    (let [node1  (sut/new-node-object node-data1 cluster)
          probe  (sut/new-probe node1 "1.2.3.4" 5568)
          result (.prepare probe)]

      (testing "Prepare ProbeEvent to vector"
        (match result [(:probe sut/event-code) (.-id probe) (.-host probe) (.-port probe) (.-restart_counter probe) (.-tx probe) (.-neighbour_host probe) (.-neighbour_port probe)]))

      (testing "Restore ProbeEvent from vector"

        (let [v            [9
                            #uuid "00000000-0000-0000-0000-000000000001"
                            "127.0.0.1"
                            5376
                            7
                            0
                            "1.2.3.4"
                            5568]
              result-probe (.restore (sut/empty-probe) v)]

          (is (= ProbeEvent (type result-probe)) "Should be ProbeEvent type")

          (is (= result-probe probe) "Restored ProbeEvent should be equals to original event")

          (is (thrown-with-msg? Exception #"ProbeEvent vector has invalid structure"
                (.restore (sut/empty-probe) [])))

          (testing "Wrong command type code"
            (is (thrown-with-msg? Exception #"ProbeEvent vector has invalid structure"
                  (.restore (sut/empty-probe) [999
                                               #uuid "00000000-0000-0000-0000-000000000001"
                                               "127.0.0.1"
                                               5376
                                               7
                                               0
                                               "1.2.3.4"
                                               5568])))))))))



(deftest build-anti-entropy-data-test
  (let [node1 (sut/new-node-object node-data1 cluster)]

    (testing "Anti entropy is build successfully"
      ;; add new normal neighbour in map
      (.upsert_neighbour node1 (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000002"
                                                        :host            "127.0.0.1"
                                                        :port            5432
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter 2
                                                        :tx              2
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))
      (.upsert_neighbour node1 (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000003"
                                                        :host            "127.0.0.1"
                                                        :port            5433
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter 3
                                                        :tx              3
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))
      (.upsert_neighbour node1 (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000004"
                                                        :host            "127.0.0.1"
                                                        :port            5434
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter 4
                                                        :tx              4
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}))
      (.upsert_neighbour node1 (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000005"
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



(deftest map->AntiEntropy-test
  (testing "AntiEntropy"
    (let [node1    (sut/new-node-object node-data1 cluster)
          nb-map   {:id              #uuid "00000000-0000-0000-0000-000000000002"
                    :host            "127.0.0.1"
                    :port            5432
                    :status          :alive
                    :access          :direct
                    :restart-counter 2
                    :tx              2
                    :payload         {}
                    :updated-at      1661799880969}
          _        (.upsert_neighbour node1 (sut/new-neighbour-node nb-map))
          ae-event (sut/new-anti-entropy node1)
          result   (.prepare ae-event)]

      (testing "Prepare AntiEntropy to vector"
        (match result [(:anti-entropy sut/event-code) (.-anti_entropy_data ae-event)]))

      (testing "Restore AntiEntropy from vector"

        (let [v         [8 [nb-map]]
              result-ae (.restore (sut/empty-anti-entropy) v)]

          (is (= AntiEntropy (type result-ae)) "Should be AntiEntropy type")

          (is (=
                (dissoc (-> result-ae :anti-entropy-data first) :updated-at)
                (dissoc (-> ae-event :anti-entropy-data first) :updated-at))
            "Restored AntiEntropy should be equals to original event")

          (is (thrown-with-msg? Exception #"AntiEntropy vector has invalid structure"
                (.restore (sut/empty-anti-entropy) [])))

          (testing "Wrong command type code"
            (is (thrown-with-msg? Exception #"AntiEntropy vector has invalid structure"
                  (.restore (sut/empty-anti-entropy) [999
                                                      [{:id              #uuid "00000000-0000-0000-0000-000000000002"
                                                        :host            "127.0.0.1"
                                                        :port            5432
                                                        :status          :alive
                                                        :access          :direct
                                                        :restart-counter 2
                                                        :tx              2
                                                        :payload         {}
                                                        :updated-at      (System/currentTimeMillis)}]])))))))))
