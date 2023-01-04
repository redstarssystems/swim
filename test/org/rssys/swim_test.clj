(ns org.rssys.swim-test
  (:require
    [clojure.datafy :refer [datafy]]
    [clojure.spec.alpha :as s]
    [clojure.string :as string]
    [clojure.test :refer [deftest is testing]]
    [matcho.core :as m]
    [org.rssys.encrypt :as e]
    [org.rssys.event :as event]
    [org.rssys.spec :as spec]
    [org.rssys.swim :as sut])
  (:import
    (clojure.lang
      IMeta)
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




;;;;;;;;;;;;;;;;;;;;;
;; Constants and data
;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *max-test-timeout*
  "Max promise timeout ms in tests"
  1500)


(defmacro no-timeout-check
  "Test check if attempt of (deref *p) has no timeout *max-test-timeout*.
  If timeout occurred then :timeout value will be delivered to promise and test will not pass."
  [*p]
  `(when-not (m/dessert :timeout (deref ~*p *max-test-timeout* :timeout))
     (deliver ~*p :timeout)))


(defn print-ex
  [^Exception e]
  (println
    (ex-message e) ":"
    (:cause (datafy (ex-cause e)))))


(def values [1 128 "hello" nil :k {:a 1 :b true} [1234567890 1]])


(def cluster-data
  {:id           #uuid "10000000-0000-0000-0000-000000000000"
   :name         "cluster1"
   :desc         "Test cluster1"
   :secret-token "0123456789abcdef0123456789abcdef"
   :nspace       "test-ns1"
   :cluster-size 3
   :tags         #{"dc1" "rssys"}})


(def node1-nb-data
  {:id              #uuid"00000000-0000-0000-0000-000000000001"
   :host            "127.0.0.1"
   :port            5376
   :status          :alive
   :access          :direct
   :restart-counter 7
   :tx              0
   :payload         {:tcp-port 4567}
   :updated-at      (System/currentTimeMillis)})


(def node2-nb-data
  {:id              #uuid"00000000-0000-0000-0000-000000000002"
   :host            "127.0.0.1"
   :port            5377
   :status          :alive
   :access          :direct
   :restart-counter 3
   :tx              0
   :payload         {:tcp-port 4567}
   :updated-at      (System/currentTimeMillis)})


(def node3-nb-data
  {:id              #uuid"00000000-0000-0000-0000-000000000003"
   :host            "127.0.0.1"
   :port            5378
   :status          :alive
   :access          :direct
   :restart-counter 5
   :tx              0
   :payload         {:tcp-port 4567}
   :updated-at      (System/currentTimeMillis)})



(def node1-data {:id #uuid "00000000-0000-0000-0000-000000000001" :host "127.0.0.1" :port 5376 :restart-counter 7})
(def node2-data {:id #uuid "00000000-0000-0000-0000-000000000002" :host "127.0.0.1" :port 5377 :restart-counter 5})
(def node3-data {:id #uuid "00000000-0000-0000-0000-000000000003" :host "127.0.0.1" :port 5378 :restart-counter 6})


(def cluster (sut/new-cluster cluster-data))


;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic functions tests
;;;;;;;;;;;;;;;;;;;;;;;;


(deftest safe-test
  (testing "should prevent any exceptions"
    (m/assert nil (sut/safe (/ 1 0))))
  (testing "should return value for any normal expression"
    (m/assert 1/2 (sut/safe (/ 1 2)))))


(deftest calc-n-test
  (testing "should calculate correct number of nodes for notification depending on cluster size"
    (let [nodes-in-cluster [1 2 4 8 16 32 64 128 256 512 1024]
          result           (mapv sut/calc-n nodes-in-cluster)]
      (m/assert [0 1 2 3 4 5 6 7 8 9 10] result))))


(deftest serialize-test
  (testing "serialization of different types"
    (let [svalues (->> values (map sut/serialize))]

      (testing "should produce a byte array for every serialized value"
        (is (every? bytes? svalues)))

      (testing "should produce output with expected size for every serialized value"
        (m/assert [6 7 11 6 9 11 7] (mapv count svalues))))))


(deftest deserialize-test
  (testing "Deserialization of different types"
    (let [bvalues (map byte-array
                    '([-110 -93 126 35 39 1]
                       [-110 -93 126 35 39 -52 -128]
                       [-110 -93 126 35 39 -91 104 101 108 108 111]
                       [-110 -93 126 35 39 -64]
                       [-110 -93 126 35 39 -93 126 58 107]
                       [-126 -93 126 58 97 1 -93 126 58 98 -61]
                       [-110, -50, 73, -106, 2, -46, 1]))
          dvalues (mapv sut/deserialize bvalues)]
      (testing "should produce output equals to initial values"
        (m/assert values dvalues)))))



(deftest nodes-in-cluster-test
  (testing "Calculate nodes in the cluster"
    (let [this (sut/new-node-object node1-data cluster)
          nb   (sut/new-neighbour-node node2-nb-data)]

      (testing "should return one node"
        (m/assert 1 (sut/nodes-in-cluster this)))

      (testing "should return two nodes"
        (sut/upsert-neighbour this nb)
        (m/assert 2 (sut/nodes-in-cluster this))))))


;;;;;;;;;;;;;;;;;;;;;;
;; Basic objects tests
;;;;;;;;;;;;;;;;;;;;;;


(deftest new-cluster-test
  (testing "Create Cluster instance"
    (let [result (sut/new-cluster cluster-data)]

      (testing "should produce correct type"
        (m/assert Cluster (type result)))

      (testing "should produce correct structure"
        (m/assert ::spec/cluster result))

      (testing "should have expected cluster size"
        (m/assert 3 (.-cluster_size result))))))



(deftest new-neighbour-node-test
  (testing "Create new neighbour node"
    (let [result1 (sut/new-neighbour-node node2-nb-data)
          result2 (sut/new-neighbour-node #uuid "00000000-0000-0000-0000-000000000000" "127.0.0.1" 5379)]

      (testing "should produce NeighbourNode instance"
        (m/assert NeighbourNode (type result1))
        (m/assert NeighbourNode (type result2)))

      (testing "should produce output with correct structure"
        (m/assert ::spec/neighbour-node result1)
        (m/assert ::spec/neighbour-node result2))

      (testing "should produce expected values"
        (m/assert node2-nb-data result1)
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
          result2)))))




(deftest new-node-object-test
  (testing "Create new node"
    (let [node-object
          (sut/new-node-object
            {:id   #uuid "00000000-0000-0000-0000-000000000001"
             :host "127.0.0.1"
             :port 5376}
            cluster)]

      (testing "should produce NodeObject instance"
        (is (instance? NodeObject node-object)))

      (testing "should produce output with correct structure"
        (m/assert ::spec/node (sut/get-value node-object)))

      (testing "should produce expected values"
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

  (testing "should catch invalid node data"
    (is (thrown-with-msg? Exception #"Invalid node data"
          (sut/new-node-object {:a 1} cluster)))))


;;;;;;;;;;;;;;;;
;; Getters tests
;;;;;;;;;;;;;;;;


(deftest getters-test
  (testing "Getters"
    (let [this (sut/new-node-object node1-data cluster)]

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

      (sut/upsert-neighbour this (sut/new-neighbour-node node2-nb-data))

      (m/assert {#uuid "00000000-0000-0000-0000-000000000002"
                 (dissoc (sut/new-neighbour-node node2-nb-data) :updated-at)}
        (sut/get-neighbours this))

      (m/assert true (sut/neighbour-exist? this #uuid "00000000-0000-0000-0000-000000000002"))
      (m/assert false (sut/neighbour-exist? this #uuid "00000000-0000-0000-0000-000000000999"))

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
      (m/assert [] (sut/get-outgoing-events this))

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

      (m/assert ^:matcho/strict
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



;;;;;;;;;;;;;;;
;; Setter tests
;;;;;;;;;;;;;;;

(deftest set-cluster-test
  (testing "Set new cluster"
    (let [new-cluster (sut/new-cluster (assoc cluster-data :id (random-uuid) :name "cluster2"))
          this        (sut/new-node-object node1-data cluster)]

      (testing "should set new cluster value for node"
        (sut/set-cluster this new-cluster)
        (m/assert (sut/get-cluster this) new-cluster))

      (testing "should catch invalid data"
        (is (thrown-with-msg? Exception #"Invalid cluster data"
              (sut/set-cluster this (assoc cluster :id 1)))))

      (testing "should prevent cluster change in all statuses except :stop"
        (is (thrown-with-msg? Exception #"Node is not stopped"
              (sut/set-status this :left)
              (sut/set-cluster this new-cluster)))))))


(deftest set-cluster-size-test
  (testing "Set cluster size"
    (let [this (sut/new-node-object node1-data cluster)
          new-cluster-size 5]

      (testing "should set cluster size to a new value"
        (m/dessert new-cluster-size (sut/get-cluster-size this))
        (sut/set-cluster-size this new-cluster-size)
        (m/assert new-cluster-size (sut/get-cluster-size this)))

      (testing "should catch invalid cluster size"
        (is (thrown-with-msg? Exception #"Invalid cluster size"
              (sut/set-cluster-size this -1)))
        (is (thrown-with-msg? Exception #"Invalid cluster size"
              (sut/set-cluster-size this 0)))))))


(deftest cluster-size-exceed?-test

  (testing "cluster size check"
    (let [this (sut/new-node-object node1-data (assoc cluster :cluster-size 1))]

      (testing "should detect cluster size exceed"
        (m/assert true? (sut/cluster-size-exceed? this)))

      (testing "should detect cluster size not exceed"
        (sut/set-cluster-size this 3)
        (m/assert false? (sut/cluster-size-exceed? this))))))



(deftest set-status-test
  (testing "set status"
    (let [this (sut/new-node-object node1-data cluster)
          new-status :left]

      (testing "should set node status to a new value"
        (m/dessert new-status (sut/get-status this))
        (sut/set-status this new-status)
        (m/assert new-status (sut/get-status this)))

      (testing "should catch invalid node status"
        (is (thrown-with-msg? Exception #"Invalid node status"
              (sut/set-status this :wrong-value)))))))


(deftest set-payload-test
  (testing "Set payload"
    (let [this        (sut/new-node-object node1-data cluster)
          new-payload {:tcp-port 1234 :role "data node"}]

      (testing "should set new payload value"
        (m/dessert new-payload (sut/get-payload this))
        (sut/set-payload this new-payload)
        (m/assert new-payload (sut/get-payload this)))

      (testing "should rise an exception if payload size is too big "
        (is (thrown-with-msg? Exception #"Payload size is too big"
              (sut/set-payload this
                {:long-string (apply str (repeat (:max-payload-size @sut/*config) "a"))})))))))


(deftest set-restart-counter-test
  (testing "set restart counter"
    (let [this (sut/new-node-object node1-data cluster)
          new-restart-counter 42]

      (testing "should set a new value"
        (m/dessert new-restart-counter (sut/get-restart-counter this))
        (sut/set-restart-counter this new-restart-counter)
        (m/assert new-restart-counter (sut/get-restart-counter this)))

      (testing "should catch invalid values"
        (is (thrown-with-msg? Exception #"Invalid restart counter data"
              (sut/set-restart-counter this -1)))))))


(deftest inc-tx-test
  (testing "increment tx"
    (let [this        (sut/new-node-object node1-data cluster)
          current-tx (sut/get-tx this)]

      (testing "should set a new tx value (+1)"
        (sut/inc-tx this)
        (m/assert (inc current-tx) (sut/get-tx this))))))


(deftest upsert-neighbour-test
  (testing "upsert neighbour"
    (let [this (sut/new-node-object node1-data (assoc cluster :cluster-size 99))]

      (testing "should insert two neighbours"
        (m/assert empty? (sut/get-neighbours this))
        (sut/upsert-neighbour this (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour this (sut/new-neighbour-node node3-nb-data))
        (m/assert 2 (count (sut/get-neighbours this)))
        (m/assert {(:id node2-nb-data) (dissoc node2-nb-data :updated-at)
                   (:id node3-nb-data) (dissoc node3-nb-data :updated-at)}
          (sut/get-neighbours this)))

      (testing "should reject neighbour with the same id as this node id"
        (m/assert 2 (count (sut/get-neighbours this)))
        (m/assert (sut/get-id this) (:id node1-nb-data))
        (sut/upsert-neighbour this node1-nb-data)
        (m/assert 2 (count (sut/get-neighbours this)))
        (m/assert {(:id node2-nb-data) (dissoc node2-nb-data :updated-at)
                   (:id node3-nb-data) (dissoc node3-nb-data :updated-at)}
          (sut/get-neighbours this)))

      (testing "should catch invalid neighbour data"
        (is (thrown-with-msg? Exception #"Invalid neighbour node data"
              (sut/upsert-neighbour this {:a :bad-value}))))

      (testing "should update neighbour's timestamp after every update"
        (let [nb-id (:id node2-nb-data)
              nb        (sut/get-neighbour this nb-id)
              old-timestamp (:updated-at nb)]
          (sut/upsert-neighbour this nb)
          (let [modified-nb (sut/get-neighbour this nb-id)]
            (is (> (:updated-at modified-nb) old-timestamp))))

        (testing "should have cluster size control"
          (let [new-cluster-size 2
                this (sut/new-node-object node1-data (assoc cluster :cluster-size new-cluster-size))]

            (testing "should success before cluster size exceed"
              (m/assert 0 (count (sut/get-neighbours this)))
              (sut/upsert-neighbour this (sut/new-neighbour-node node2-nb-data))
              (m/assert 1 (count (sut/get-neighbours this))))

            (testing "should fail because cluster size exceed"
              (is (thrown-with-msg? Exception #"Cluster size exceeded"
                    (sut/upsert-neighbour this node1-nb-data))))

            (testing "should success if neighbour exist"
              (let [new-restart-counter 4]
                (sut/upsert-neighbour this (assoc node2-nb-data :restart-counter new-restart-counter))
                (m/assert
                  {:restart-counter new-restart-counter}
                  (sut/get-neighbour this (:id node2-nb-data)))))))))))


(deftest delete-neighbour-test
  (testing "delete neighbour"
    (let [this (sut/new-node-object node1-data cluster)
          _    (sut/upsert-neighbour this (sut/new-neighbour-node node2-nb-data))
          _    (sut/upsert-neighbour this (sut/new-neighbour-node node3-nb-data))
          nb-count (count (sut/get-neighbours this))]

      (testing "should delete first neighbour"
        (sut/delete-neighbour this (:id node2-nb-data))
        (m/assert (dec nb-count) (count (sut/get-neighbours this))))

      (testing "neighbours map should contain second neighbour"
        (m/assert ^:matcho/strict
          {(:id node3-nb-data) (dissoc node3-nb-data :updated-at)}
          (sut/get-neighbours this)))

      (testing "should delete second neighbour"
        (sut/delete-neighbour this (:id node3-nb-data))
        (m/assert 0 (count (sut/get-neighbours this)))))))


(deftest delete-neighbours-test
  (testing "delete neighbours"
    (let [this (sut/new-node-object node1-data cluster)
          _    (sut/upsert-neighbour this (sut/new-neighbour-node node2-nb-data))
          _    (sut/upsert-neighbour this (sut/new-neighbour-node node3-nb-data))]

      (testing "should delete all neighbours from neighbours map"
        (m/assert 2 (count (sut/get-neighbours this)))
        (m/assert 2 (sut/delete-neighbours this))
        (m/assert 0 (count (sut/get-neighbours this)))))))


(deftest set-outgoing-events-test
  (testing "set outgoing events"
    (let [new-outgoing-events [[(:left event/code) (random-uuid)]]
          this                (sut/new-node-object node1-data cluster)]

      (testing "should set new value"
        (m/dessert new-outgoing-events (sut/get-outgoing-events this))
        (sut/set-outgoing-events this new-outgoing-events)
        (m/assert ^:matcho/strict new-outgoing-events (sut/get-outgoing-events this))))))



(deftest put-event-test
  (testing "put event"
    (let [this       (sut/new-node-object node1-data cluster)
          ping-event (event/empty-ping)
          ack-event  (event/empty-ack)
          expected-events [ping-event ack-event]]

      (testing "should save two events in outgoing events vector"
        (m/dessert expected-events (sut/get-outgoing-events this))
        (sut/put-event this (event/empty-ping))
        (sut/put-event this (event/empty-ack))
        (m/assert ^:matcho/strict expected-events (sut/get-outgoing-events this))))))


(deftest take-events-test
  (testing "take events"
    (let [this (sut/new-node-object node1-data cluster)
          ping-event (event/empty-ping)
          ack-event  (event/empty-ack)
          expected-events [ping-event ack-event]]

      (sut/put-event this ping-event)
      (sut/put-event this ack-event)

      (testing "should return two events from outgoing events buffer"
        (m/assert ^:matcho/strict expected-events (sut/take-events this 2)))

      (testing "should delete taken events from outgoing events buffer"
        (m/assert empty? (sut/get-outgoing-events this)))

      (testing "without arguments should take all events and make buffer empty"
        (sut/put-event this ping-event)
        (sut/put-event this ack-event)
        (m/assert expected-events (sut/take-events this))
        (m/assert empty? (sut/get-outgoing-events this))))))


(deftest upsert-ping-test
  (testing "upsert ping"
    (let [this (sut/new-node-object node1-data cluster)
          ping-event (event/empty-ping)
          ping-id (.-neighbour_id ping-event)]

      (testing "should return key (`neighbour-id`) after upsert"
        (m/assert nil (sut/get-ping-event this ping-id))
        (m/assert ping-id (sut/upsert-ping this ping-event)))

      (testing "should save ping event in a ping events map"
        (m/assert ping-event (sut/get-ping-event this ping-id)))

      (testing "should increment attempt number if ping event is already in a map"
        (sut/upsert-ping this ping-event)
        (sut/upsert-ping this ping-event)
        (m/assert {:attempt-number 3} (sut/get-ping-event this ping-id)))

      (testing "should catch invalid ping event data"
        (is (thrown-with-msg? Exception #"Invalid ping event data"
              (sut/upsert-ping this {:a :bad-value})))))))


(deftest delete-ping-test
  (testing "delete ping"
    (let [this (sut/new-node-object node1-data cluster)
          ping-event (event/empty-ping)
          ping-id (.-neighbour_id ping-event)]

      (sut/upsert-ping this ping-event)

      (testing "should delete ping event from ping events map"
        (m/assert ping-event (sut/get-ping-event this ping-id))
        (sut/delete-ping this ping-id)
        (m/assert nil (sut/get-ping-event this ping-id))))))


(deftest upsert-indirect-ping-test

  (testing "upsert indirect ping"
    (let [this (sut/new-node-object node1-data cluster)
          indirect-ping-event (event/empty-indirect-ping)
          ping-id (.-neighbour_id indirect-ping-event)]

      (testing "should return key (`neighbour-id`) after upsert"
        (m/assert nil (sut/get-indirect-ping-event this ping-id))
        (m/assert ping-id (sut/upsert-indirect-ping this indirect-ping-event)))

      (testing "should save indirect ping event in a map"
        (m/assert indirect-ping-event (sut/get-indirect-ping-event this ping-id)))

      (testing "should increment attempt number if indirect ping event is already in a map"
        (sut/upsert-indirect-ping this indirect-ping-event)
        (sut/upsert-indirect-ping this indirect-ping-event)
        (m/assert {:attempt-number 3} (sut/get-indirect-ping-event this ping-id)))

      (testing "should catch invalid indirect ping event data"
        (is (thrown-with-msg? Exception #"Invalid indirect ping event data"
              (sut/upsert-indirect-ping this {:a :bad-value})))))))


(deftest delete-indirect-ping-test
  (testing "delete indirect ping"
    (let [this (sut/new-node-object node1-data cluster)
          indirect-ping-event (event/empty-indirect-ping)
          ping-id (.-neighbour_id indirect-ping-event)]

      (sut/upsert-indirect-ping this indirect-ping-event)

      (testing "should delete indirect ping event from map"
        (m/assert indirect-ping-event (sut/get-indirect-ping-event this ping-id))
        (sut/delete-indirect-ping this ping-id)
        (m/assert nil (sut/get-indirect-ping-event this ping-id))))))


(deftest insert-probe-test
  (testing "insert probe"
    (let [this (sut/new-node-object node1-data cluster)
          probe-event (event/empty-probe)
          probe-key (.-probe_key probe-event)]

      (testing "should save probe key from probe event in a probe events map"
        (m/assert false (contains? (sut/get-probe-events this) probe-key))
        (sut/insert-probe this probe-event)
        (m/assert true (contains? (sut/get-probe-events this) probe-key))
        (m/assert {probe-key nil} (sut/get-probe-events this)))

      (testing "should catch invalid probe event data"
        (is (thrown-with-msg? Exception #"Invalid probe event data"
              (sut/insert-probe this {:a :bad-value})))))))


(deftest delete-probe-test
  (testing "delete probe"
    (let [this (sut/new-node-object node1-data cluster)
          probe-event (event/empty-probe)
          probe-key (.-probe_key probe-event)]

      (sut/insert-probe this probe-event)

      (testing "should delete probe key from probe events map"
        (m/assert ^:matcho/strict {probe-key nil} (sut/get-probe-events this))
        (sut/delete-probe this probe-key)
        (m/dessert ^:matcho/strict {probe-key nil} (sut/get-probe-events this))))))


(deftest upsert-probe-ack-test
  (testing "upsert ack probe"
    (let [this            (sut/new-node-object node1-data cluster)
          probe-event     (event/empty-probe)
          probe-ack-event (sut/new-probe-ack-event this probe-event)
          probe-key       (.-probe_key probe-event)]

      (testing "should save probe ack event using probe key"
        (m/dessert {probe-key probe-ack-event} (sut/get-probe-events this))
        (sut/upsert-probe-ack this probe-ack-event)
        (m/assert {probe-key probe-ack-event} (sut/get-probe-events this)))

      (testing "should update existing probe ack event using probe key"
        (sut/upsert-probe-ack this (assoc probe-ack-event :restart-counter 42))
        (m/assert {probe-key {:restart-counter 42}} (sut/get-probe-events this)))

      (testing "should catch invalid probe ack data"
        (is (thrown-with-msg? Exception #"Invalid probe ack event data"
              (sut/upsert-probe-ack this {:a :bad-value})))))))


;;;;;;;;;;;;;;;;;;;;;;;
;; Event builders tests
;;;;;;;;;;;;;;;;;;;;;;;


(deftest new-probe-event-test
  (testing "probe event builder"
    (let [this        (sut/new-node-object node1-data cluster)
          current-tx  (sut/get-tx this)
          probe-event (sut/new-probe-event this "127.0.0.1" 5376)]

      (testing "should generate event with correct type"
        (m/assert ProbeEvent (type probe-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/probe-event probe-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch invalid probe data"
        (is (thrown-with-msg? Exception #"Invalid probe data"
              (sut/new-probe-event this "127.0.01" -1)))))))



(deftest new-probe-ack-event-test
  (testing "probe ack event builder"
    (let [this            (sut/new-node-object node1-data cluster)
          probe-event     (sut/new-probe-event this "127.0.0.1" 5376)
          current-tx      (sut/get-tx this)
          probe-ack-event (sut/new-probe-ack-event this probe-event)]

      (testing "should generate event with correct type"
        (m/assert ProbeAckEvent (type probe-ack-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/probe-ack-event probe-ack-event))

      (testing "should set probe key to probe ack event from probe event"
        (m/assert (.-probe_key probe-event) (.-probe_key probe-ack-event)))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch invalid probe ack data"
        (is (thrown-with-msg? Exception #"Invalid probe ack data"
              (sut/new-probe-ack-event this (assoc probe-event :id :bad-value))))))))


(deftest new-ping-event-test
  (testing "ping event builder"
    (let [this       (sut/new-node-object node1-data cluster)
          current-tx (sut/get-tx this)
          ping-event (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)]

      (testing "should generate event with correct type"
        (m/assert PingEvent (type ping-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/ping-event ping-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch invalid ping data"
        (is (thrown-with-msg? Exception #"Invalid ping data"
              (sut/new-ping-event this :bad-value 42)))))))


(deftest new-ack-event-test
  (testing "ack event builder"
    (let [this       (sut/new-node-object node1-data cluster)
          ping-event (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)
          current-tx (sut/get-tx this)
          ack-event  (sut/new-ack-event this ping-event)]

      (testing "should generate event with correct type"
        (m/assert AckEvent (type ack-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/ack-event ack-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch invalid ack data"
        (is (thrown-with-msg? Exception #"Invalid ack data"
              (sut/new-ack-event this (assoc ping-event :id :bad-value))))))))



(deftest new-indirect-ping-event-test

  (testing "indirect ping builder"
    (let [this                (sut/new-node-object node1-data cluster)
          intermediate        (sut/new-neighbour-node node2-nb-data)
          neighbour           (sut/new-neighbour-node node3-nb-data)
          _                   (sut/upsert-neighbour this intermediate)
          _                   (sut/upsert-neighbour this neighbour)
          current-tx          (sut/get-tx this)
          indirect-ping-event (sut/new-indirect-ping-event this (:id intermediate) (:id neighbour) 1)]


      (testing "should generate event with correct type"
        (m/assert IndirectPingEvent (type indirect-ping-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/indirect-ping-event indirect-ping-event)
        (m/assert {:intermediate-id   (:id intermediate)
                   :intermediate-host (:host intermediate)
                   :intermediate-port (:port intermediate)}
          indirect-ping-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch unknown intermediate node"
        (is (thrown-with-msg? Exception #"Unknown intermediate node with such id"
              (sut/new-indirect-ping-event this :bad-id (:id neighbour) 1))))

      (testing "should catch unknown neighbour"
        (is (thrown-with-msg? Exception #"Unknown neighbour node with such id"
              (sut/new-indirect-ping-event this (:id intermediate) :bad-id 1))))

      (testing "should catch invalid indirect ping data"
        (is (thrown-with-msg? Exception #"Invalid indirect ping data"
              (sut/new-indirect-ping-event this (:id intermediate) (:id neighbour) :bad-value)))))))



(deftest new-indirect-ack-event-test

  (testing "indirect ack builder"
    (let [this                (sut/new-node-object node1-data cluster)
          intermediate        (sut/new-neighbour-node node2-nb-data)
          neighbour           (sut/new-neighbour-node node3-nb-data)
          _                   (sut/upsert-neighbour this intermediate)
          _                   (sut/upsert-neighbour this neighbour)
          indirect-ping-event (sut/new-indirect-ping-event this (:id intermediate) (:id neighbour) 1)
          ack-node            (sut/new-node-object node3-data cluster)
          current-tx          (sut/get-tx ack-node)
          indirect-ack-event  (sut/new-indirect-ack-event ack-node indirect-ping-event)]

      (testing "should generate event with correct type"
        (m/assert IndirectAckEvent (type indirect-ack-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/indirect-ack-event indirect-ack-event)
        (m/assert {:intermediate-id   (:id intermediate)
                   :intermediate-host (:host intermediate)
                   :intermediate-port (:port intermediate)}
          indirect-ack-event)
        (m/assert {:neighbour-id   (sut/get-id this)
                   :neighbour-host (sut/get-host this)
                   :neighbour-port (sut/get-port this)}
          indirect-ack-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx ack-node)))

      (testing "should catch invalid indirect ack data"
        (is (thrown-with-msg? Exception #"Invalid indirect ack data"
              (sut/new-indirect-ack-event ack-node (assoc indirect-ping-event :attempt-number -1))))))))


(deftest new-alive-event-test

  (testing "alive event builder"
    (let [this           (sut/new-node-object node1-data cluster)
          neighbour-this (sut/new-node-object node2-data cluster)
          _              (sut/upsert-neighbour this (sut/new-neighbour-node node2-nb-data))
          ping-event     (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)
          ack-event      (sut/new-ack-event neighbour-this ping-event)
          current-tx     (sut/get-tx this)
          alive-event    (sut/new-alive-event this ack-event)]

      (testing "should generate event with correct type"
        (m/assert AliveEvent (type alive-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/alive-event alive-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch invalid alive event data"
        (is (thrown-with-msg? Exception #"Invalid alive data"
              (sut/new-alive-event this (assoc ack-event :id :bad-value))))))))



(deftest new-cluster-size-event-test
  (testing "new cluster size event builder"
    (let [this             (sut/new-node-object node1-data cluster)
          new-cluster-size 5
          current-tx       (sut/get-tx this)
          ncs-event        (sut/new-cluster-size-event this new-cluster-size)]

      (testing "should generate event with correct type"
        (m/assert NewClusterSizeEvent (type ncs-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/new-cluster-size-event ncs-event)
        (m/assert {:old-cluster-size (.-cluster_size cluster)
                   :new-cluster-size new-cluster-size}
          ncs-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch invalid cluster size data"
        (is (thrown-with-msg? Exception #"Invalid cluster size data"
              (sut/new-cluster-size-event this -1)))))))



(deftest new-dead-event-test

  (testing "dead event builder"
    (let [this       (sut/new-node-object node1-data cluster)
          ping-event (sut/new-ping-event this #uuid "00000000-0000-0000-0000-000000000002" 1)
          neighbour  (sut/new-neighbour-node node2-nb-data)
          _          (sut/upsert-neighbour this neighbour)
          current-tx (sut/get-tx this)
          dead-event (sut/new-dead-event this (.-neighbour_id ping-event))]

      (testing "should generate event with correct type"
        (m/assert DeadEvent (type dead-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/dead-event dead-event)
        (m/assert {:neighbour-id #uuid "00000000-0000-0000-0000-000000000002"}
          dead-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch invalid dead event data"
        (is (thrown-with-msg? Exception #"Invalid dead event data"
              (sut/new-dead-event this (assoc ping-event :id :bad-value))))))))


(deftest neighbour->vec-test
  (testing "convert NeighbourNode to vector of values"
    (let [nb (sut/new-neighbour-node node2-nb-data)]
      (testing "should return values in correct order"
        (m/assert [#uuid "00000000-0000-0000-0000-000000000002" ;; id
                   "127.0.0.1"                                  ;; host
                   5377                                         ;; port
                   3                                            ;; :alive = 3
                   0                                            ;; 0 - direct, 1 - indirect
                   3                                            ;; restart-counter
                   0                                            ;; tx
                   {:tcp-port 4567}]                            ;; payload
          (sut/neighbour->vec nb))))))



(deftest vec->neighbour-test
  (testing "Convert vector of values to NeighbourNode"
    (let [v      [#uuid "00000000-0000-0000-0000-000000000002" ;; id
                  "127.0.0.1"                               ;; host
                  5377                                      ;; port
                  3                                         ;; :alive = 3
                  0                                         ;; 0 - direct, 1 - indirect
                  4                                         ;; restart-counter
                  42                                         ;; tx
                  {:tcp-port 4567}]

          result (sut/vec->neighbour v)]

      (testing "should return NeighbourNode object"
        (m/assert true (instance? NeighbourNode result)))

      (testing "should return correct values"
        (m/assert ^:matcho/strict
          {:id              #uuid "00000000-0000-0000-0000-000000000002"
           :access          :direct
           :host            "127.0.0.1"
           :payload         {:tcp-port 4567}
           :port            5377
           :restart-counter 4
           :status          :alive
           :tx              42
           :updated-at      0}
          result))

      (testing "should catch invalid vector data"
        (is (thrown-with-msg? Exception #"Invalid data in vector for NeighbourNode"
              (sut/vec->neighbour [1 2 3 4 :bad-values])))))))



(deftest build-anti-entropy-data-test
  (testing "build vector with anti-entropy data"
    (let [node1                   (sut/new-node-object node1-data cluster)
          neighbour1              (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000002"
                                                           :host            "127.0.0.1"
                                                           :port            5432
                                                           :status          :alive
                                                           :access          :direct
                                                           :restart-counter 2
                                                           :tx              2
                                                           :payload         {}
                                                           :updated-at      (System/currentTimeMillis)})
          neighbour2              (sut/new-neighbour-node {:id              #uuid "00000000-0000-0000-0000-000000000003"
                                                           :host            "127.0.0.1"
                                                           :port            5433
                                                           :status          :alive
                                                           :access          :direct
                                                           :restart-counter 3
                                                           :tx              3
                                                           :payload         {}
                                                           :updated-at      (System/currentTimeMillis)})

          requested-size          1
          without-neighbours-node (sut/new-node-object node2-data cluster)]

      (sut/upsert-neighbour node1 neighbour1)
      (sut/upsert-neighbour node1 neighbour2)

      (testing "should return vector with :max-anti-entropy-items items by default"
        (m/assert (:max-anti-entropy-items @sut/*config) (count (sut/build-anti-entropy-data node1))))

      (testing "should return vector with requested number of items"
        (m/assert requested-size (count (sut/build-anti-entropy-data node1 :num requested-size))))

      (testing "should return empty vector for node without neighbours"
        (m/assert [] (sut/build-anti-entropy-data without-neighbours-node)))

      (testing "should return vector with values for node2 and node3"
        (m/assert [[#uuid "00000000-0000-0000-0000-000000000002" "127.0.0.1" 5432 3 0 2 2 {}]
                   [#uuid "00000000-0000-0000-0000-000000000003" "127.0.0.1" 5433 3 0 3 3 {}]]
          (sort (sut/build-anti-entropy-data node1)))

        (testing "should return vector with requested value for node3"
          (m/assert [[#uuid "00000000-0000-0000-0000-000000000003" "127.0.0.1" 5433 3 0 3 3 {}]]
            (sut/build-anti-entropy-data node1 {:neighbour-id (:id node3-data)})))

        (testing "should return empty vector if requested unknown node"
          (m/assert []
            (sut/build-anti-entropy-data node1 {:neighbour-id 123})))))))


(deftest new-anti-entropy-event-test

  (testing "anti entropy event builder"
    (let [this                (sut/new-node-object node1-data cluster)
          neighbour           (sut/new-neighbour-node node2-nb-data)
          _                   (sut/upsert-neighbour this neighbour)
          _                   (sut/upsert-neighbour this (sut/new-neighbour-node node3-nb-data))
          current-tx          (sut/get-tx this)
          anti-entropy-event  (sut/new-anti-entropy-event this)
          current-tx2          (sut/get-tx this)
          anti-entropy-event2 (sut/new-anti-entropy-event this {:neighbour-id (:id node2-nb-data)})]

      (testing "should generate event with correct type"
        (m/assert AntiEntropy (type anti-entropy-event))
        (m/assert AntiEntropy (type anti-entropy-event2)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/anti-entropy-event anti-entropy-event)
        (m/assert ::spec/anti-entropy-event anti-entropy-event2))

      (testing "should generate event with number of :max-anti-entropy-items if :neighbour-id parameter omitted "
        (m/assert
          (:max-anti-entropy-items @sut/*config)
          (count (:anti-entropy-data anti-entropy-event))))

      (testing "should generate event with particular neighbour info if :neighbour-id parameter present"
        (m/assert
          {:anti-entropy-data [[(:id node2-nb-data) "127.0.0.1" 5377 3 0 3 0 {:tcp-port 4567}]]}
          anti-entropy-event2))

      (testing "should increase tx"
        (m/assert (inc current-tx) current-tx2)
        (m/assert (inc current-tx2) (sut/get-tx this))))))



(deftest new-join-event-test

  (testing "join event builder"
    (let [this       (sut/new-node-object node1-data cluster)
          current-tx (sut/get-tx this)
          join-event (sut/new-join-event this)]

      (testing "should generate event with correct type"
        (m/assert JoinEvent (type join-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/join-event join-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this))))))



(deftest new-suspect-event-test

  (testing "suspect event builder"
    (let [this          (sut/new-node-object node1-data cluster)
          neighbour     (sut/new-neighbour-node node2-nb-data)
          _             (sut/upsert-neighbour this neighbour)
          current-tx    (sut/get-tx this)
          suspect-event (sut/new-suspect-event this (:id neighbour))]

      (testing "should generate event with correct type"
        (m/assert SuspectEvent (type suspect-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/suspect-event suspect-event)
        (m/assert {:neighbour-id              (:id neighbour)
                   :neighbour-restart-counter (:restart-counter neighbour)
                   :neighbour-tx              (:tx neighbour)}
          suspect-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this)))

      (testing "should catch invalid suspect event data"
        (is (thrown-with-msg? Exception #"Invalid suspect event data"
              (sut/new-suspect-event this :bad-value)))))))



(deftest new-left-event-test

  (testing "left event builder"
    (let [this       (sut/new-node-object node1-data cluster)
          current-tx (sut/get-tx this)
          left-event (sut/new-left-event this)]

      (testing "should generate event with correct type"
        (m/assert LeftEvent (type left-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/left-event left-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this))))))



(deftest new-payload-event-test
  (testing "payload event builder"
    (let [this          (sut/new-node-object node1-data cluster)
          payload       {:a 1 :b [3]}
          _             (sut/set-payload this payload)
          current-tx    (sut/get-tx this)
          payload-event (sut/new-payload-event this)]

      (testing "should generate event with correct type"
        (m/assert PayloadEvent (type payload-event)))

      (testing "should generate event with correct structure"
        (m/assert ::spec/payload-event payload-event)
        (m/assert {:payload payload} payload-event))

      (testing "should increase tx"
        (m/assert (inc current-tx) (sut/get-tx this))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Utility functions tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(deftest suitable-restart-counter?-test
  (testing "check restart counter in event"
    (let [node1      (sut/new-node-object node1-data cluster)
          node2      (sut/new-node-object node2-data cluster)
          ping-event (sut/new-ping-event node1 (sut/get-id node2) 42)]

      (testing "should accept normal restart counter"
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))
        (m/assert true? (sut/suitable-restart-counter? node2 ping-event))

        (testing "should reject outdated restart counter"
          (sut/upsert-neighbour node2 (sut/new-neighbour-node (update node1-nb-data :restart-counter inc)))
          (m/assert false (sut/suitable-restart-counter? node2 ping-event)))

        (testing "should not crash if neighbour or event is nil or absent"
          (m/assert nil (sut/suitable-restart-counter? node2 nil))
          (m/assert nil (sut/suitable-restart-counter? node2 {:id 123})))))))


(deftest suitable-tx?-test
  (testing "check tx in event"
    (let [node1      (sut/new-node-object node1-data cluster)
          node2      (sut/new-node-object node2-data cluster)
          _          (sut/inc-tx node1)
          ping-event (sut/new-ping-event node1 (sut/get-id node2) 42)]

      (testing "should accept normal tx"
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))
        (m/assert true? (sut/suitable-tx? node2 ping-event)))

      (testing "should reject outdated tx"
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (update node1-nb-data :tx inc)))
        (m/assert false (sut/suitable-tx? node2 ping-event)))

      (testing "should not crash if neighbour or event is nil or absent"
        (m/assert nil (sut/suitable-tx? node2 nil))
        (m/assert nil (sut/suitable-tx? node2 {:id 123}))))))


(deftest suitable-incarnation?-test
  (testing "check incarnation in event"
    (let [node1      (sut/new-node-object node1-data cluster)
          node2      (sut/new-node-object node2-data cluster)
          _          (sut/inc-tx node1)
          ping-event (sut/new-ping-event node1 (sut/get-id node2) 42)]

      (testing "should accept normal incarnation"
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))
        (m/assert true (sut/suitable-incarnation? node2 ping-event)))

      (testing "should reject cause tx is outdated"
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (update node1-nb-data :tx inc)))
        (m/assert false (sut/suitable-incarnation? node2 ping-event)))

      (testing "should reject cause restart counter is outdated"
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (update node1-nb-data :restart-counter inc)))
        (m/assert false (sut/suitable-incarnation? node2 ping-event)))

      (testing "should not crash if neighbour or event is nil or absent"
        (m/assert false (sut/suitable-incarnation? node2 nil))
        (m/assert false (sut/suitable-incarnation? node2 {:id 123}))))))



(deftest get-neighbours-with-status-test
  (testing "get neighbours with status"
    (let [this                 (sut/new-node-object node1-data (assoc cluster :cluster-size 99))
          new-node-with-status (fn [status] (sut/new-neighbour-node (assoc node3-nb-data :id (random-uuid) :status status)))]

      (run!
        #(sut/upsert-neighbour this (new-node-with-status %))
        [:left :stop :alive :alive :dead :dead :dead :suspect :suspect :suspect :suspect])

      (testing "should return all alive and left neighbours"
        (let [result (filterv (fn [nb] (#{:alive :left} (:status nb)))
                       (sut/get-neighbours-with-status this #{:alive :left}))]
          (m/assert 3 (count result))))

      (testing "should return all alive neighbours"
        (let [result (filterv (fn [nb] (#{:alive} (:status nb))) (sut/get-alive-neighbours this))]
          (m/assert 2 (count result))))

      (testing "should return all stopped neighbours"
        (let [result (filterv (fn [nb] (#{:stop} (:status nb))) (sut/get-stopped-neighbours this))]
          (m/assert 1 (count result))))

      (testing "should return all dead neighbours"
        (let [result (filterv (fn [nb] (#{:dead} (:status nb))) (sut/get-dead-neighbours this))]
          (m/assert 3 (count result))))

      (testing "should return all left neighbours"
        (let [result (filterv (fn [nb] (#{:left} (:status nb))) (sut/get-left-neighbours this))]
          (m/assert 1 (count result))))

      (testing "should return all suspect neighbours"
        (let [result (filterv (fn [nb] (#{:suspect} (:status nb))) (sut/get-suspect-neighbours this))]
          (m/assert 4 (count result)))))))



(deftest get-oldest-neighbour-test
  (testing "get oldest neighbour"
    (let [this        (sut/new-node-object node1-data (assoc cluster :cluster-size 99))
          oldest-id   #uuid"00000000-0000-0000-0000-000000000555"
          oldest-1-id #uuid"00000000-0000-0000-0000-000000000777"
          oldest-2-id #uuid"00000000-0000-0000-0000-000000000222"
          oldest-3-id #uuid"00000000-0000-0000-0000-000000000111"
          oldest-nb   (sut/new-neighbour-node (assoc node3-nb-data :id oldest-id))
          oldest-1-nb (sut/new-neighbour-node (assoc node3-nb-data :id oldest-1-id))
          oldest-2-nb (sut/new-neighbour-node (assoc node3-nb-data :id oldest-2-id))
          oldest-3-nb (sut/new-neighbour-node (assoc node3-nb-data :status :left :id oldest-3-id))]

      (sut/upsert-neighbour this oldest-nb)
      (Thread/sleep 1)
      (sut/upsert-neighbour this oldest-1-nb)
      (Thread/sleep 1)
      (sut/upsert-neighbour this oldest-2-nb)
      (Thread/sleep 1)
      (sut/upsert-neighbour this oldest-3-nb)

      (testing "should return oldest"
        (m/assert {:id oldest-id} (sut/get-oldest-neighbour this)))

      (testing "after delete oldest should return oldest-1"
        (sut/delete-neighbour this (:id oldest-nb))
        (m/assert {:id oldest-1-id} (sut/get-oldest-neighbour this)))

      (testing "with status :left should return oldest-3"
        (m/assert {:id oldest-3-id} (sut/get-oldest-neighbour this #{:left}))))))



(deftest alive-neighbour?-test

  (testing "should be true for neighbours with alive statuses "
    (let [nb1 (sut/new-neighbour-node node2-nb-data)
          nb2 (sut/new-neighbour-node (assoc node3-nb-data :status :suspect))]
      (m/assert true (sut/alive-neighbour? nb1))
      (m/assert true (sut/alive-neighbour? nb2))))

  (testing "should be false for neighbours with not alive statuses"
    (let [nb1 (sut/new-neighbour-node (assoc node2-nb-data :status :left))
          nb2 (sut/new-neighbour-node (assoc node3-nb-data :status :dead))
          nb3 (sut/new-neighbour-node (assoc node1-nb-data :status :stop))]
      (m/assert false (sut/alive-neighbour? nb1))
      (m/assert false (sut/alive-neighbour? nb2))
      (m/assert false (sut/alive-neighbour? nb3)))))


(deftest alive-node?-test

  (testing "should be true for nodes with alive statuses"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)]
      (sut/set-alive-status node1)
      (sut/set-suspect-status node2)
      (m/assert true (sut/alive-node? node1))
      (m/assert true (sut/alive-node? node2))))

  (testing "should be false for nodes with not alive statuses"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (sut/set-dead-status node1)
      (sut/set-left-status node2)
      (sut/set-stop-status node2)
      (m/assert false (sut/alive-node? node1))
      (m/assert false (sut/alive-node? node2))
      (m/assert false (sut/alive-node? node3)))))



(deftest set-nb-tx-test

  (testing "set tx for neighbour"
    (let [this   (sut/new-node-object node1-data cluster)
          nb     (sut/new-neighbour-node (assoc node2-nb-data :status :left))
          new-tx 42]
      (sut/upsert-neighbour this nb)

      (testing "should update tx"
        (m/assert 0 (:tx (sut/get-neighbour this (.-id nb))))
        (sut/set-nb-tx this (.-id nb) new-tx)
        (m/assert new-tx (:tx (sut/get-neighbour this (.-id nb)))))

      (testing "should do nothing if new tx less or equals to current value"
        (m/assert new-tx (:tx (sut/get-neighbour this (.-id nb))))
        (sut/set-nb-tx this (.-id nb) 0)
        (m/assert new-tx (:tx (sut/get-neighbour this (.-id nb))))

        (sut/set-nb-tx this (.-id nb) new-tx)
        (m/assert new-tx (:tx (sut/get-neighbour this (.-id nb))))))))



(deftest set-nb-restart-counter-test

  (testing "set restart counter for neighbour"
    (let [this                (sut/new-node-object node1-data cluster)
          nb                  (sut/new-neighbour-node (assoc node2-nb-data :status :left))
          new-restart-counter 42]
      (sut/upsert-neighbour this nb)

      (testing "should update restart counter for neighbour"
        (m/assert (:restart-counter node2-nb-data) (:restart-counter (sut/get-neighbour this (.-id nb))))
        (sut/set-nb-restart-counter this (.-id nb) new-restart-counter)
        (m/assert new-restart-counter (:restart-counter (sut/get-neighbour this (.-id nb)))))

      (testing "should do nothing if new restart counter less or equals to current value"
        (m/assert new-restart-counter (:restart-counter (sut/get-neighbour this (.-id nb))))
        (sut/set-nb-restart-counter this (.-id nb) 0)
        (m/assert new-restart-counter (:restart-counter (sut/get-neighbour this (.-id nb))))

        (sut/set-nb-restart-counter this (.-id nb) new-restart-counter)
        (m/assert new-restart-counter (:restart-counter (sut/get-neighbour this (.-id nb))))))))



(deftest set-nb-status-test

  (testing "set status for neighbour"
    (let [this           (sut/new-node-object node1-data cluster)
          current-status :left
          nb             (sut/new-neighbour-node (assoc node2-nb-data :status current-status))
          new-status     :alive]
      (sut/upsert-neighbour this nb)

      (testing "should update status for neighbour"
        (m/assert current-status (:status (sut/get-neighbour this (.-id nb))))
        (sut/set-nb-status this (.-id nb) new-status)
        (m/assert new-status (:status (sut/get-neighbour this (.-id nb))))))))



(deftest set-nb-direct-access-test

  (testing "set direct access for neighbour"
    (let [this            (sut/new-node-object node1-data cluster)
          indirect-access :indirect
          nb              (sut/new-neighbour-node (assoc node2-nb-data :access indirect-access))
          direct-access   :direct]
      (sut/upsert-neighbour this nb)

      (testing "should set to :direct value for neighbour"
        (m/assert indirect-access (:access (sut/get-neighbour this (.-id nb))))
        (sut/set-nb-direct-access this (.-id nb))
        (m/assert direct-access (:access (sut/get-neighbour this (.-id nb))))))))


(deftest set-nb-indirect-access-test

  (testing "set indirect access for neighbour"
    (let [this            (sut/new-node-object node1-data cluster)
          direct-access   :direct
          nb              (sut/new-neighbour-node (assoc node2-nb-data :access direct-access))
          indirect-access :indirect]
      (sut/upsert-neighbour this nb)

      (testing "should set to :indirect value for neighbour"
        (m/assert direct-access (:access (sut/get-neighbour this (.-id nb))))
        (sut/set-nb-indirect-access this (.-id nb))
        (m/assert indirect-access (:access (sut/get-neighbour this (.-id nb))))))))


;;;;;;;;;;;;;;;;;;;
;; Send event tests
;;;;;;;;;;;;;;;;;;;


(defn empty-node-process-fn
  "Run empty node process"
  [^NodeObject this]
  (while (-> this sut/get-value :*udp-server deref :continue?)
    (Thread/sleep 5)))


(defn set-incoming-data-to-payload-processor-fn
  "Set received events to payload section without processing them."
  [^NodeObject this ^bytes encrypted-data]
  (let [secret-key     (-> this sut/get-cluster :secret-key)
        decrypted-data (sut/safe (e/decrypt-data ^bytes secret-key ^bytes encrypted-data))
        events-vector  (sut/deserialize ^bytes decrypted-data)]
    (sut/set-payload this events-vector)))



(deftest send-events-test
  (testing "send events"
    (let [node1  (sut/new-node-object node1-data (assoc cluster :cluster-size 999))
          node2  (sut/new-node-object node2-data (assoc cluster :cluster-size 999))
          event1 (sut/new-probe-event node1 (sut/get-host node2) (sut/get-port node2))
          event2 (event/empty-ack)
          event3 (sut/new-anti-entropy-event node1)
          events [event1 event2 event3]]
      (try
        (sut/start node2 empty-node-process-fn set-incoming-data-to-payload-processor-fn)
        (sut/start node1 empty-node-process-fn #(do [%1 %2]))

        (testing "should transform all given events and send them to node2"
          (let [*expecting-event (promise)
                event-catcher-fn (fn [v]
                                   (when-let [cmd (:org.rssys.swim/cmd v)]
                                     (when (and
                                             (= :set-payload cmd)
                                             (= (:id node2-data) (:node-id v)))
                                       (deliver *expecting-event v))))]
            (add-tap event-catcher-fn)
            (m/assert pos-int? (sut/send-events node1 events (:host node2-data) (:port node2-data)))
            (no-timeout-check *expecting-event)
            (m/assert [(.prepare event1) (.prepare event2) (.prepare event3)] (sut/get-payload node2))
            (remove-tap event-catcher-fn)))

        (testing "should rise an exception if UDP packet size is too big"
          (is (thrown-with-msg? Exception #"UDP packet is too big"
                (sut/send-events node1 events (:host node2-data) (:port node2-data)
                  {:max-udp-size 1}))))

        (testing "should ignore UDP size check and send events anyway"
          (m/assert pos-int? (sut/send-events node1 events (:host node2-data) (:port node2-data)
                               {:max-udp-size 1 :ignore-max-udp-size? true})))

        (catch Exception e
          (print-ex e))
        (finally
          (sut/stop node1)
          (sut/stop node2))))))


(deftest send-event-test

  (testing "send one event"
    (let [this  (sut/new-node-object node1-data cluster)
          neighbour (sut/new-neighbour-node node2-nb-data)
          event (sut/new-cluster-size-event this 5)]
      (try
        (sut/start this empty-node-process-fn #(do [%1 %2]))
        (sut/upsert-neighbour this neighbour)

        (testing "using host:port should return number of bytes sent"
          (m/assert pos-int? (sut/send-event this event (:host node2-nb-data) (:port node2-nb-data))))

        (testing "using neighbour id should return number of bytes sent"
          (m/assert pos-int? (sut/send-event this event (:id neighbour))))

        (testing "should rise an exception if neighbour id is unknown"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/send-event this event :bad-id))))

        (catch Exception e
          (print-ex e))
        (finally
          (sut/stop this))))))



(deftest send-event-ae-test
  (testing "send one event + anti-entropy data"
    (let [this  (sut/new-node-object node1-data cluster)
          neighbour (sut/new-neighbour-node node2-nb-data)
          event (sut/new-cluster-size-event this 5)]
      (try
        (sut/start this empty-node-process-fn #(do [%1 %2]))
        (sut/upsert-neighbour this neighbour)

        (testing "using host:port"
          (let [with-anti-entropy-size (sut/send-event-ae this event (:host node2-nb-data) (:port node2-nb-data))
                without-anti-entropy-size (sut/send-event this event (:host node2-nb-data) (:port node2-nb-data))]

            (testing "should return number of bytes sent"
              (m/assert pos-int? with-anti-entropy-size))

            (testing "should return size lager than sending event without anti-entropy data"
              (m/assert true (> with-anti-entropy-size without-anti-entropy-size)))))

        (testing "using neighbour id"
          (let [with-anti-entropy-size (sut/send-event-ae this event (:id neighbour))
                without-anti-entropy-size (sut/send-event this event (:id neighbour))]

            (testing "should return number of bytes sent"
              (m/assert pos-int? with-anti-entropy-size))

            (testing "should return size lager than sending event without anti-entropy data"
              (m/assert true (> with-anti-entropy-size without-anti-entropy-size)))))

        (testing "should rise an exception if neighbour id is unknown"
          (is (thrown-with-msg? Exception #"Unknown neighbour id"
                (sut/send-event-ae this event :bad-id))))

        (catch Exception e
          (print-ex e))
        (finally
          (sut/stop this))))))


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Event processing tests
;;;;;;;;;;;;;;;;;;;;;;;;;

(defn set-event-catcher
  "Set new event catcher for given node.
  Returns pair [*event-promise event-tap-fn], where `event-tap-fn` should catch event on given node via `tap>` mechanism.
  Expected event will be delivered to *event-promise. Use `no-timeout-check` macro to detect promise timeout.
  `event-tap-fn` is already bound to 'tap>' mechanism.
  NB: Don't forget to call remove-tap for `event-tap-fn` after test."
  [node-id event-kw]
  (let [*p    (promise)
        tap-f (fn [v]
                (when-let [cmd (:org.rssys.swim/cmd v)]
                  (when (and
                          (= cmd event-kw)
                          (= node-id (:node-id v)))
                    (deliver *p v))))]
    (add-tap tap-f)
    [*p tap-f]))


(deftest ^:event-processing probe-test

  (testing "send probe event"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node2-id (sut/get-id node2)
          [*e1 e1-tap-fn] (set-event-catcher node2-id :udp-packet-processor)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

        (let [probe-key (sut/probe node1 (sut/get-host node2) (sut/get-port node2))]

          (no-timeout-check *e1)

          (testing "should return probe key as UUID"
            (m/assert UUID (type probe-key)))

          (testing "should put probe key to probe events map"
            (is (contains? (sut/get-probe-events node1) probe-key)))

          (testing "node2 should receive probe event with expected probe key"
            (m/assert {:data {:event {:probe-key probe-key}} :node-id node2-id} @*e1)))

        (catch Exception e
          (print-ex e))
        (finally
          (remove-tap e1-tap-fn)
          (sut/stop node1)
          (sut/stop node2))))))


(deftest ^:event-processing probe-event-test

  (testing "probe event processing"
    (testing "positive case"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :probe-ack-event)
            [*e2 e2-tap-fn] (set-event-catcher node1-id :upsert-probe-ack)
            [*e3 e3-tap-fn] (set-event-catcher node1-id :upsert-neighbour)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (testing "should receive ProbeAck event on node1"
            (let [probe-key (sut/probe node1 (sut/get-host node2) (sut/get-port node2))]

              (testing "should be ProbeAck event from node2"
                (no-timeout-check *e1)
                (m/assert {:data {:id node2-id} :node-id node1-id} @*e1))

              (testing "should insert ProbeAck response into probe event map under probe key"
                (no-timeout-check *e2)
                (m/assert {:id           node2-id
                           :neighbour-id node1-id
                           :probe-key    probe-key}
                  (sut/get-probe-event node1 probe-key)))

              (testing "should upsert neighbour from ProbeAck when status is not alive"
                (no-timeout-check *e3)
                (m/assert node2-data (sut/get-neighbour node1 node2-id)))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (remove-tap e2-tap-fn)
            (remove-tap e3-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "should not upsert neighbour if cluster size limit exceed"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :upsert-neighbour-cluster-size-exceeded-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-cluster-size node1 1)
          (sut/probe node1 (sut/get-host node2) (sut/get-port node2))

          (testing "should rise error on upsert neighbour"
            (no-timeout-check *e1)
            (m/assert {:data {:cluster-size 1, :nodes-in-cluster 1} :node-id node1-id} @*e1)
            (m/assert nil (sut/get-neighbour node1 node2-id)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "should not process ProbeAck if Probe event was never sent"
      (let [node1       (sut/new-node-object node1-data cluster)
            node2       (sut/new-node-object node2-data cluster)
            probe-event (sut/new-probe-event node1 (:host node2-data) (:port node2-data))
            node1-id    (sut/get-id node1)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :probe-ack-event-probe-never-send-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/send-event node2 (sut/new-probe-ack-event node2 probe-event) (:host node1-data) (:port node1-data))

          (testing "should rise an error and have no probe ack events"
            (no-timeout-check *e1)
            (m/assert empty? (sut/get-probe-events node1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "should not upsert neighbour from ProbeAck when status is alive"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :upsert-probe-ack)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/set-alive-status node1)

          (testing "should receive ProbeAck event on node1"
            (let [probe-key (sut/probe node1 (sut/get-host node2) (sut/get-port node2))]

              (testing "should insert ProbeAck response into probe event map under probe key"
                (no-timeout-check *e1)
                (m/assert {:id           node2-id
                           :neighbour-id node1-id
                           :probe-key    probe-key}
                  (sut/get-probe-event node1 probe-key)))

              (testing "neighbours map should be empty"
                (m/assert empty? (sut/get-neighbours node1)))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))))



(deftest ^:event-processing anti-entropy-event-test
  (testing "anti-entropy event processing"
    (testing "positive case"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (:id node3-nb-data)
            [*e1 e1-tap-fn] (set-event-catcher node2-id :anti-entropy-event)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (testing "node2 should have one neighbour node1 before anti-entropy event "
            (m/assert 1 (count (sut/get-neighbours node2)))
            (m/assert {node1-id {}} (sut/get-neighbours node2)))

          (sut/send-event node1 (sut/new-anti-entropy-event node1) (:host node2-data) (:port node2-data))

          (testing "node2 should receive AntiEntropy event"
            (no-timeout-check *e1)
            (m/assert {:node-id node2-id} @*e1))

          (testing "node2 should have two neighbours: node1 and node3 after anti-entropy event"
            (m/assert 2 (count (sut/get-neighbours node2)))
            (m/assert {node1-id {} node3-id {}} (sut/get-neighbours node2)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "should do nothing if anti-entropy event from unknown node"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node2-id :anti-entropy-event-unknown-neighbour-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (testing "node2 should have zero neighbours before anti-entropy event "
            (m/assert 0 (count (sut/get-neighbours node2))))

          (sut/send-event node1 (sut/new-anti-entropy-event node1) (:host node2-data) (:port node2-data))
          (no-timeout-check *e1)

          (testing "node2 should have zero neighbours after anti-entropy event "
            (m/assert 0 (count (sut/get-neighbours node2))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "should do nothing if anti-entropy event has bad restart counter"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node2-id :anti-entropy-event-bad-restart-counter-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node
                                        (assoc node1-nb-data :restart-counter 999)))

          (testing "node2 should have one neighbour before anti-entropy event "
            (m/assert 1 (count (sut/get-neighbours node2))))

          (sut/send-event node1 (sut/new-anti-entropy-event node1) (:host node2-data) (:port node2-data))
          (no-timeout-check *e1)

          (testing "node2 should have one neighbour after anti-entropy event "
            (m/assert 1 (count (sut/get-neighbours node2))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "should do nothing if anti-entropy event has bad tx"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node2-id :anti-entropy-event-bad-tx-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node
                                        (assoc node1-nb-data :tx 999)))

          (testing "node2 should have one neighbour before anti-entropy event "
            (m/assert 1 (count (sut/get-neighbours node2))))

          (sut/send-event node1 (sut/new-anti-entropy-event node1) (:host node2-data) (:port node2-data))
          (no-timeout-check *e1)

          (testing "node2 should have one neighbour after anti-entropy event "
            (m/assert 1 (count (sut/get-neighbours node2))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "should do nothing if anti-entropy event from not alive node"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node2-id :anti-entropy-event-not-alive-neighbour-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))
          (sut/set-nb-stop-status node2 node1-id)

          (testing "node2 should have one neighbour before anti-entropy event "
            (m/assert 1 (count (sut/get-neighbours node2))))

          (sut/send-event node1 (sut/new-anti-entropy-event node1) (:host node2-data) (:port node2-data))
          (no-timeout-check *e1)

          (testing "node2 should have one neighbour after anti-entropy event "
            (m/assert 1 (count (sut/get-neighbours node2))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))))


(deftest ^:event-processing indirect-ack-event-test

  (testing "indirect ack event processing"

    (testing "should accept event on node1"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :indirect-ack-event)
            [*e2 e2-tap-fn] (set-event-catcher node2-id :intermediate-node-indirect-ack-event)
            [*e3 e3-tap-fn] (set-event-catcher node1-id :put-event)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

          (sut/set-nb-suspect-status node1 node3-id)

          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                node3-tx            (.-tx (sut/get-neighbour node1 node3-id))
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)
                indirect-ack-event  (sut/new-indirect-ack-event node3 indirect-ping-event)]

            (testing "node1 should have indirect ping request in indirect ping map before process indirect ack event"
              (m/assert
                {:id node1-id :intermediate-id node2-id :neighbour-id node3-id}
                (sut/get-indirect-ping-event node1 node3-id)))

            (testing "node1 should have direct access (default) for neighbour node3 before process indirect ack event"
              (m/assert :direct (:access (sut/get-neighbour node1 node3-id))))

            (testing "node1 should have suspect status for neighbour node3 before process indirect ack event"
              (m/assert :suspect (:status (sut/get-neighbour node1 node3-id))))

            (sut/send-event node3 indirect-ack-event node2-id)

            (testing "node2 as intermediate node should receive event for node1 from node3"
              (no-timeout-check *e2))

            (testing "node1 should receive indirect ack event from node3"
              (no-timeout-check *e1))

            (testing "node1 should set tx for neighbour node3 by tx value from event"
              (m/assert (-> @*e1 :data :tx) (.-tx (sut/get-neighbour node1 node3-id)))
              (m/assert true (> (.-tx (sut/get-neighbour node1 node3-id)) node3-tx)))

            (testing "node1 should set indirect access for neighbour node3"
              (m/assert :indirect (:access (sut/get-neighbour node1 node3-id))))

            (testing "node1 should set new status of neighbour node3 by value from event"
              (m/assert :alive (:status (sut/get-neighbour node1 node3-id))))

            (testing "node1 should delete indirect ping request from indirect ping map"
              (m/assert nil (sut/get-indirect-ping-event node1 node3-id)))

            (testing "node1 should put alive event about node3 to outgoing events buffer"
              (no-timeout-check *e3)
              (m/assert {:node-id node1-id
                         :data    {:event {:neighbour-id node3-id :cmd-type 3}}} @*e3)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (remove-tap e2-tap-fn)
            (remove-tap e3-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event if its status is not alive"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :indirect-ack-event-not-alive-node-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-left-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)
                indirect-ack-event  (sut/new-indirect-ack-event node3 indirect-ping-event)]

            (sut/send-event node3 indirect-ack-event node2-id)

            (testing "node1 should reject indirect ack event"
              (no-timeout-check *e1))

            (testing "indirect ping event should be still in map"
              (m/dessert empty? (sut/get-indirect-ping-event node1 node3-id))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "intermediate node should reject event if its status is not alive"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node2-id :indirect-ack-event-not-alive-node-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-left-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)
                indirect-ack-event  (sut/new-indirect-ack-event node3 indirect-ping-event)]

            (sut/send-event node3 indirect-ack-event node2-id)

            (testing "node2 as intermediate node should reject indirect ack event"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event from unknown neighbour"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :indirect-ack-event-unknown-neighbour-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)
                indirect-ack-event  (sut/new-indirect-ack-event node3 indirect-ping-event)]


            (sut/delete-neighbour node1 node3-id)           ;; make node3 unknown for node1
            (sut/send-event node3 indirect-ack-event node2-id)

            (testing "node1 should reject indirect ack event from unknown node3"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event with bad restart counter"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :indirect-ack-event-bad-restart-counter-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)
                indirect-ack-event  (sut/new-indirect-ack-event node3 indirect-ping-event)]

            (sut/set-nb-restart-counter node1 node3-id 999)
            (sut/send-event node3 indirect-ack-event node2-id)

            (testing "node1 should reject indirect ack event from node3 with outdated restart counter"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event with bad tx"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :indirect-ack-event-bad-tx-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)
                indirect-ack-event  (sut/new-indirect-ack-event node3 indirect-ping-event)]

            (sut/set-nb-tx node1 node3-id 999)
            (sut/send-event node3 indirect-ack-event node2-id)

            (testing "node1 should reject indirect ack event from node3 with outdated tx"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event if indirect ping request was never sent"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :indirect-ack-event-not-expected-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                indirect-ack-event  (sut/new-indirect-ack-event node3 indirect-ping-event)]

            (sut/send-event node3 indirect-ack-event node2-id)

            (testing "node1 should reject indirect ack event from node3"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))))


(deftest ^:event-processing indirect-ping-event-test

  (testing "indirect ping event processing"

    (testing "should accept event on node3"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node3-id :indirect-ping-event)
            [*e2 e2-tap-fn] (set-event-catcher node3-id :indirect-ack-event)
            [*e3 e3-tap-fn] (set-event-catcher node3-id :send-events-udp-size)
            [*e4 e4-tap-fn] (set-event-catcher node2-id :intermediate-node-indirect-ping-event)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                node1-tx            (.-tx (sut/get-neighbour node3 node1-id))
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)]

            (sut/send-event node1 indirect-ping-event node2-id)

            (testing "node2 as intermediate node should receive event for node3 from node1"
              (no-timeout-check *e4))

            (testing "node3 should receive indirect ping event from node1"
              (no-timeout-check *e1)
              (m/assert {:node-id node3-id
                         :data    {:id node1-id
                                   :intermediate-id node2-id
                                   :neighbour-id node3-id}}
                @*e1))

            (testing "node3 should generate indirect ack event for node1"
              (no-timeout-check *e2)
              (m/assert {:node-id node3-id
                         :data    {:id node3-id
                                   :intermediate-id node2-id
                                   :neighbour-id node1-id}}
                @*e2))

            (testing "node3 should set tx for neighbour node1 by tx value from event"
              (m/assert (-> @*e1 :data :tx) (.-tx (sut/get-neighbour node3 node1-id)))
              (m/assert true (> (.-tx (sut/get-neighbour node3 node1-id)) node1-tx)))

            (testing "node3 should send indirect ack event for node1"
              (no-timeout-check *e3)
              (m/assert {:node-id node3-id
                         :data    {:udp-size pos-int?}} @*e3)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (remove-tap e2-tap-fn)
            (remove-tap e3-tap-fn)
            (remove-tap e4-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event if its status is not alive"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node3-id :indirect-ping-event-not-alive-node-error)
            [*e4 e4-tap-fn] (set-event-catcher node2-id :intermediate-node-indirect-ping-event)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-left-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                node1-tx            (.-tx (sut/get-neighbour node3 node1-id))
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)]

            (sut/send-event node1 indirect-ping-event node2-id)

            (testing "node2 as intermediate node should receive event for node3 from node1"
              (no-timeout-check *e4))

            (testing "node3 should reject indirect ping event from node1"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (remove-tap e4-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "intermediate node should reject event if its status is not alive"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node2-id :indirect-ping-event-not-alive-node-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-left-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)]

            (sut/send-event node1 indirect-ping-event node2-id)

            (testing "node2 as intermediate node should reject indirect ping event"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "should reject event from unknown neighbour"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node3-id :indirect-ping-event-unknown-neighbour-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)]

            (sut/delete-neighbour node3 node1-id)           ;; make node1 unknown for node3
            (sut/send-event node1 indirect-ping-event node2-id)

            (testing "node3 should reject indirect ping event from unknown node1"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event with bad restart counter"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node3-id :indirect-ping-event-bad-restart-counter-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)]

            (sut/set-nb-restart-counter node3 node1-id 999)
            (sut/send-event node1 indirect-ping-event node2-id)

            (testing "node3 should reject indirect ping event from node1 with outdated restart counter"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event with bad tx"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node3-id :indirect-ping-event-bad-tx-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)]

            (sut/set-nb-tx node3 node1-id 999)
            (sut/send-event node1 indirect-ping-event node2-id)

            (testing "node3 should reject indirect ping event from node1 with outdated tx"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))

    (testing "destination node should reject event intended for different node"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node3    (sut/new-node-object node3-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            node3-id (sut/get-id node3)
            [*e1 e1-tap-fn] (set-event-catcher node3-id :indirect-ping-event-neighbour-id-mismatch-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)
          (sut/set-alive-status node3)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))
          (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))


          (let [indirect-ping-event (sut/new-indirect-ping-event node1 node2-id node3-id 1)
                _                   (sut/upsert-indirect-ping node1 indirect-ping-event)]

            (sut/send-event node1 (assoc indirect-ping-event :neighbour-id #uuid "00000000-0000-0000-0000-000000000999") node2-id)

            (testing "node3 should reject indirect ping event from node1 with different node id"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)
            (sut/stop node3)))))))


(deftest ^:event-processing ack-event-test

  (testing "ack event processing"

    (testing "should accept event on node1"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :ack-event)
            [*e2 e2-tap-fn] (set-event-catcher node1-id :put-event)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (sut/set-nb-suspect-status node1 node2-id)

          (let [ping-event (sut/new-ping-event node1 node2-id 1)
                node2-tx   (.-tx (sut/get-neighbour node1 node2-id))
                _          (sut/upsert-ping node1 ping-event)
                ack-event  (sut/new-ack-event node2 ping-event)]

            (testing "node1 should have ping request in ping map before process ack event"
              (m/assert
                {:id node1-id :neighbour-id node2-id}
                (sut/get-ping-event node1 node2-id)))

            (testing "node1 should have suspect status for neighbour node2 before process ack event"
              (m/assert :suspect (:status (sut/get-neighbour node1 node2-id))))

            (sut/send-event node2 ack-event node1-id)

            (testing "node1 should receive ack event from node2"
              (no-timeout-check *e1))

            (testing "node1 should set tx for neighbour node2 by tx value from event"
              (m/assert (-> @*e1 :data :tx) (.-tx (sut/get-neighbour node1 node2-id)))
              (m/assert true (> (.-tx (sut/get-neighbour node1 node2-id)) node2-tx)))

            (testing "node1 should set new status of neighbour node2 by value from event"
              (m/assert :alive (:status (sut/get-neighbour node1 node2-id))))

            (testing "node1 should delete ping request from ping map"
              (m/assert nil (sut/get-ping-event node1 node2-id)))

            (testing "node1 should put alive event about node2 to outgoing events buffer"
              (no-timeout-check *e2)
              (m/assert {:node-id node1-id
                         :data    {:event {:neighbour-id node2-id :cmd-type 3}}} @*e2)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (remove-tap e2-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "destination node should reject event if its status is not alive"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :ack-event-not-alive-node-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-left-status node1)
          (sut/set-alive-status node2)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (let [ping-event (sut/new-ping-event node1 node2-id 1)
                _          (sut/upsert-ping node1 ping-event)
                ack-event  (sut/new-ack-event node2 ping-event)]

            (testing "node1 should have ping request in ping map before process ack event"
              (m/assert
                {:id node1-id :neighbour-id node2-id}
                (sut/get-ping-event node1 node2-id)))

            (sut/send-event node2 ack-event node1-id)

            (testing "node1 should reject ack event from node2 cause its status is left"
              (no-timeout-check *e1))

            (testing "ping request should be ping map"
              (m/dessert nil (sut/get-ping-event node1 node2-id))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "destination node should reject event if its status is not alive"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :ack-event-not-alive-node-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-left-status node1)
          (sut/set-alive-status node2)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (let [ping-event (sut/new-ping-event node1 node2-id 1)
                _          (sut/upsert-ping node1 ping-event)
                ack-event  (sut/new-ack-event node2 ping-event)]

            (sut/send-event node2 ack-event node1-id)

            (testing "node1 should reject ack event from node2 cause its status is left"
              (no-timeout-check *e1))

            (testing "ping request should be still in a ping map"
              (m/dessert nil (sut/get-ping-event node1 node2-id))))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "destination node should reject event from unknown neighbour"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :ack-event-unknown-neighbour-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)

          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (let [ping-event (sut/new-ping-event node1 node2-id 1)
                _          (sut/upsert-ping node1 ping-event)
                ack-event  (sut/new-ack-event node2 ping-event)]

            (sut/send-event node2 ack-event node1-id)

            (testing "node1 should reject ack event from unknown node2 "
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "destination node should reject event from not alive neighbour"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :ack-event-not-alive-neighbour-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (let [ping-event (sut/new-ping-event node1 node2-id 1)
                _          (sut/upsert-ping node1 ping-event)
                ack-event  (sut/new-ack-event node2 ping-event)]

            (sut/set-nb-left-status node1 node2-id)
            (sut/send-event node2 ack-event node1-id)

            (testing "node1 should reject ack event from not alive node2 "
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "destination node should reject event with bad restart counter"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :ack-event-bad-restart-counter-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (let [ping-event (sut/new-ping-event node1 node2-id 1)
                _          (sut/upsert-ping node1 ping-event)
                ack-event  (sut/new-ack-event node2 ping-event)]


            (sut/set-nb-restart-counter node1 node2-id 999)
            (sut/send-event node2 ack-event node1-id)

            (testing "node1 should reject ack event with outdated restart counter"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "destination node should reject event with bad tx"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :ack-event-bad-tx-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (let [ping-event (sut/new-ping-event node1 node2-id 1)
                _          (sut/upsert-ping node1 ping-event)
                ack-event  (sut/new-ack-event node2 ping-event)]


            (sut/set-nb-tx node1 node2-id 999)
            (sut/send-event node2 ack-event node1-id)

            (testing "node1 should reject ack event with outdated tx"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))

    (testing "destination node should reject event if ping request was never sent"
      (let [node1    (sut/new-node-object node1-data cluster)
            node2    (sut/new-node-object node2-data cluster)
            node1-id (sut/get-id node1)
            node2-id (sut/get-id node2)
            [*e1 e1-tap-fn] (set-event-catcher node1-id :ack-event-not-expected-error)]

        (try
          (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
          (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

          (sut/set-alive-status node1)
          (sut/set-alive-status node2)

          (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
          (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

          (let [ping-event (sut/new-ping-event node1 node2-id 1)
                ack-event  (sut/new-ack-event node2 ping-event)]

            (sut/send-event node2 ack-event node1-id)

            (testing "node1 should reject ack event from node2"
              (no-timeout-check *e1)))

          (catch Exception e
            (print-ex e))
          (finally
            (remove-tap e1-tap-fn)
            (sut/stop node1)
            (sut/stop node2)))))))



(deftest ^:logic ping-event-test

  (testing "Accept normal ping event on node2 from node1"
    (let [node1        (sut/new-node-object node1-data cluster)
          node2        (sut/new-node-object node2-data cluster)
          neighbour-id (sut/get-id node2)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :suspect)
        (sut/set-status node2 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc node1-nb-data :status :suspect)))

        (let [ping-event        (sut/new-ping-event node1 neighbour-id 1)
              _                 (sut/upsert-ping node1 ping-event)

              *expecting-event  (promise)
              event-catcher-fn  (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (= cmd :ping-event)
                                      (deliver *expecting-event v))))
              *expecting-event2 (promise)
              event-catcher-fn2 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (= cmd :ack-event)
                                      (deliver *expecting-event2 v))))
              *expecting-event3 (promise)
              event-catcher-fn3 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (= cmd :put-event)
                                      (deliver *expecting-event3 v))))

              *expecting-event4 (promise)
              event-catcher-fn4 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :upsert-neighbour)
                                            (= (sut/get-id node2) (:node-id v))
                                            (= :alive (-> v :data :neighbour-node :status))
                                            (= 1 (-> v :data :neighbour-node :tx))) ;; there may be another events in tap> queue
                                      (deliver *expecting-event4 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)
          (add-tap event-catcher-fn3)
          (add-tap event-catcher-fn4)

          (sut/send-event node1 ping-event neighbour-id)

          (no-timeout-check *expecting-event)
          (no-timeout-check *expecting-event2)
          (no-timeout-check *expecting-event3)
          (no-timeout-check *expecting-event4)

          (testing "node2 should receive ping event from node1"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :ping-event
               :data
               {:attempt-number  1
                :cmd-type        0
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                :port            5376
                :restart-counter 7
                :tx              1}}
              @*expecting-event))

          (testing "node2 should generate ack event for node1"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :ack-event
               :data
               {:attempt-number  1
                :cmd-type        1
                :id              #uuid "00000000-0000-0000-0000-000000000002"
                :neighbour-id    #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-tx    1
                :restart-counter 5
                :tx              2}}
              @*expecting-event2))

          (testing "node2 should generate and put alive event about node1"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :put-event
               :data
               {:event
                {:cmd-type                  3
                 :id                        #uuid "00000000-0000-0000-0000-000000000002"
                 :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                 :neighbour-restart-counter 7
                 :neighbour-tx              1
                 :restart-counter           5
                 :tx                        4}
                :tx 5}}
              @*expecting-event3))

          (testing "node2 should update neighbour info about node1"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :upsert-neighbour
               :data
               {:neighbour-node
                {:access          :direct
                 :host            "127.0.0.1"
                 :id              #uuid "00000000-0000-0000-0000-000000000001"
                 :payload         {:tcp-port 4567}
                 :port            5376
                 :restart-counter 7
                 :status          :alive
                 :tx              1}}}
              @*expecting-event4))

          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2)
          (remove-tap event-catcher-fn3)
          (remove-tap event-catcher-fn4))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do not accept ping event on node2 if node2 is not alive"
    (let [node1        (sut/new-node-object node1-data cluster)
          node2        (sut/new-node-object node2-data cluster)
          neighbour-id (sut/get-id node2)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :alive)
        (sut/set-status node2 :left)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

        (let [ping-event       (sut/new-ping-event node1 neighbour-id 1)
              _                (sut/upsert-ping node1 ping-event)

              *expecting-event (promise)
              event-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :ping-event-not-alive-node-error)
                                     (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          (sut/send-event node1 ping-event neighbour-id)

          (no-timeout-check *expecting-event)

          (testing "node2 should not accept ping event if its statuses are not alive"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :ping-event-not-alive-node-error
               :data
               {:attempt-number  1
                :cmd-type        0
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                :port            5376
                :restart-counter 7
                :tx              1}}
              @*expecting-event))

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do not accept ping event on node2 from unknown neighbour"
    (let [node1        (sut/new-node-object node1-data cluster)
          node2        (sut/new-node-object node2-data cluster)
          neighbour-id (sut/get-id node2)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :alive)
        (sut/set-status node2 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))

        (let [ping-event       (sut/new-ping-event node1 neighbour-id 1)
              _                (sut/upsert-ping node1 ping-event)

              *expecting-event (promise)
              event-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :ping-event-unknown-neighbour-error)
                                     (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          (sut/send-event node1 ping-event neighbour-id)

          (no-timeout-check *expecting-event)

          (testing "node2 should not accept ping event from unknown neigbour"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :ping-event-unknown-neighbour-error
               :data
               {:attempt-number  1
                :cmd-type        0
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                :port            5376
                :restart-counter 7
                :tx              1}}
              @*expecting-event))

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do not accept ping event on node2 from not alive neighbour node1"
    (let [node1        (sut/new-node-object node1-data cluster)
          node2        (sut/new-node-object node2-data cluster)
          neighbour-id (sut/get-id node2)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :alive)
        (sut/set-status node2 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc node1-nb-data :status :dead)))

        (let [ping-event        (sut/new-ping-event node1 neighbour-id 1)
              _                 (sut/upsert-ping node1 ping-event)

              *expecting-event  (promise)
              event-catcher-fn  (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (= cmd :ping-event-not-alive-neighbour-error)
                                      (deliver *expecting-event v))))
              *expecting-event2 (promise)
              event-catcher-fn2 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :udp-packet-processor)
                                            (= (sut/get-id node1) (:node-id v))
                                            (= 6 (-> v :data :event :cmd-type)))
                                      (deliver *expecting-event2 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)

          (sut/send-event node1 ping-event neighbour-id)

          (no-timeout-check *expecting-event)
          (no-timeout-check *expecting-event2)

          (testing "node2 should receive ping event from node1"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :ping-event-not-alive-neighbour-error
               :data
               {:attempt-number  1
                :cmd-type        0
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                :port            5376
                :restart-counter 7
                :tx              1}}
              @*expecting-event))


          (testing "node1 should receive dead event from node2"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :ts                 pos-int?
               :org.rssys.swim/cmd :udp-packet-processor
               :data
               {:event
                {:cmd-type                  6
                 :id                        #uuid "00000000-0000-0000-0000-000000000002"
                 :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                 :neighbour-restart-counter 7
                 :neighbour-tx              1
                 :restart-counter           5
                 :tx                        2}}}
              @*expecting-event2))

          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do not accept ping event on node2 with outdated restart counter"
    (let [node1        (sut/new-node-object node1-data cluster)
          node2        (sut/new-node-object node2-data cluster)
          neighbour-id (sut/get-id node2)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :alive)
        (sut/set-status node2 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc node1-nb-data :restart-counter 999)))

        (let [ping-event        (sut/new-ping-event node1 neighbour-id 1)
              _                 (sut/upsert-ping node1 ping-event)

              *expecting-event  (promise)
              event-catcher-fn  (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (= cmd :ping-event-bad-restart-counter-error)
                                      (deliver *expecting-event v))))
              *expecting-event2 (promise)
              event-catcher-fn2 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :udp-packet-processor)
                                            (= (sut/get-id node1) (:node-id v))
                                            (= 6 (-> v :data :event :cmd-type)))
                                      (deliver *expecting-event2 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)

          (sut/send-event node1 ping-event neighbour-id)

          (no-timeout-check *expecting-event)
          (no-timeout-check *expecting-event2)

          (testing "node2 should not accept ping event with outdated restart counter"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :ping-event-bad-restart-counter-error
               :data
               {:attempt-number  1
                :cmd-type        0
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                :port            5376
                :restart-counter 7
                :tx              1}}
              @*expecting-event))


          (testing "node1 should receive dead event from node2"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :ts                 pos-int?
               :org.rssys.swim/cmd :udp-packet-processor
               :data
               {:event
                {:cmd-type                  6
                 :id                        #uuid "00000000-0000-0000-0000-000000000002"
                 :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                 :neighbour-restart-counter 7
                 :neighbour-tx              1
                 :restart-counter           5
                 :tx                        2}}}
              @*expecting-event2))

          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do not accept ping event on node2 with outdated tx"
    (let [node1        (sut/new-node-object node1-data cluster)
          node2        (sut/new-node-object node2-data cluster)
          neighbour-id (sut/get-id node2)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :alive)
        (sut/set-status node2 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc node1-nb-data :tx 999)))

        (let [ping-event       (sut/new-ping-event node1 neighbour-id 1)
              _                (sut/upsert-ping node1 ping-event)

              *expecting-event (promise)
              event-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :ping-event-bad-tx-error)
                                     (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          (sut/send-event node1 ping-event neighbour-id)

          (no-timeout-check *expecting-event)

          (testing "node2 should not accept ping event with outdated tx"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :ping-event-bad-tx-error
               :data
               {:attempt-number  1
                :cmd-type        0
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                :port            5376
                :restart-counter 7
                :tx              1}}
              @*expecting-event))

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)))))

  (testing "Do not accept ping event on node2 for other node"
    (let [node1        (sut/new-node-object node1-data cluster)
          node2        (sut/new-node-object node2-data cluster)
          neighbour-id (sut/get-id node2)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :alive)
        (sut/set-status node2 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))

        (let [ping-event       (sut/new-ping-event node1 neighbour-id 1)
              _                (sut/upsert-ping node1 ping-event)

              *expecting-event (promise)
              event-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :ping-event-neighbour-id-mismatch-error)
                                     (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          (sut/send-event
            node1
            (assoc ping-event :neighbour-id #uuid "00000000-0000-0000-0000-000000000999")
            neighbour-id)

          (no-timeout-check *expecting-event)

          (testing "node2 should not accept ping event for other node"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000002"
               :ts                 pos-int?
               :org.rssys.swim/cmd :ping-event-neighbour-id-mismatch-error
               :data
               {:attempt-number  1
                :cmd-type        0
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-id    #uuid "00000000-0000-0000-0000-000000000999"
                :port            5376
                :restart-counter 7
                :tx              1}}
              @*expecting-event))

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2))))))


(deftest join-test

  (testing "Join node1 to a single node cluster"
    (let [node1 (sut/new-node-object node1-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/set-status node1 :left)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))

        (sut/set-cluster-size node1 1)

        (let [current-restart-counter (sut/get-restart-counter node1)

              *expecting-event        (promise)
              event-catcher-fn        (fn [v]
                                        (when-let [cmd (:org.rssys.swim/cmd v)]
                                          (when (and
                                                  (= cmd :set-status)
                                                  (= (:id node1-data) (-> v :node-id))
                                                  (= :join (-> v :data :new-status)))
                                            (deliver *expecting-event v))))

              *expecting-event2       (promise)
              event-catcher-fn2       (fn [v]
                                        (when-let [cmd (:org.rssys.swim/cmd v)]
                                          (when (= cmd :delete-neighbours)
                                            (deliver *expecting-event2 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)

          (testing "Join should return true"
            (m/assert true (sut/join node1)))

          (testing "After join, restart counter should be increased"
            (m/assert (inc current-restart-counter) (sut/get-restart-counter node1)))


          (testing "node1 should pass intermediate :join status"
            (no-timeout-check *expecting-event))

          (no-timeout-check *expecting-event2)

          (testing "node1 should delete all info about neighbours in single node cluster"
            (m/assert ^:matcho/strict
              {:org.rssys.swim/cmd :delete-neighbours
               :node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :data               {:deleted-num 2}
               :ts                 pos-int?}
              @*expecting-event2))

          (testing "Repeat join should do nothing"
            (m/assert nil (sut/join node1)))

          (testing "After join node1 should have :alive status in a single node cluster"
            (m/assert :alive (sut/get-status node1)))

          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)))))

  (testing "Join node1 to a cluster of 3 nodes"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :left)
        (sut/set-status node2 :alive)
        (sut/set-status node3 :dead)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node (assoc node3-nb-data :status :dead)))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc node3-nb-data :status :dead)))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node1-nb-data))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node node1-nb-data))

        (let [current-restart-counter (sut/get-restart-counter node1)
              *expecting-event        (promise)
              event-catcher-fn        (fn [v]
                                        (when-let [cmd (:org.rssys.swim/cmd v)]
                                          (when (and
                                                  (= cmd :set-status)
                                                  (= (:id node1-data) (-> v :node-id))
                                                  (= :join (-> v :data :new-status)))
                                            (deliver *expecting-event v))))
              *expecting-event2       (promise)
              event-catcher-fn2       (fn [v]
                                        (when-let [cmd (:org.rssys.swim/cmd v)]
                                          (when (= cmd :join)
                                            (deliver *expecting-event2 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)

          (testing "Join should return true"
            (m/assert true (sut/join node1)))

          (testing "After join, restart counter should be increased"
            (m/assert (inc current-restart-counter) (sut/get-restart-counter node1)))

          (testing "After join, node1 should have :join status"
            (no-timeout-check *expecting-event))

          (no-timeout-check *expecting-event2)

          (testing "node1 should notify alive node2 but not dead node3"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :ts                 pos-int?
               :org.rssys.swim/cmd :join
               :data
               {:notified-neighbours [#uuid "00000000-0000-0000-0000-000000000002"]}}
              @*expecting-event2))

          (testing "Repeat join should do nothing"
            (m/assert nil (sut/join node1)))

          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3))))))


(deftest ^:logic join-event-test

  (testing "Process join event from node1 on node2 or node3"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :left)
        (sut/set-status node2 :alive)
        (sut/set-status node3 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

        (let [*expecting-event  (promise)
              event-catcher-fn  (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and              ;; node2
                                            (= cmd :join-event)
                                            (= (:id node2-data) (-> v :node-id)))
                                      (deliver *expecting-event v))))
              event-catcher-fn2 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and              ;; node3
                                            (= cmd :join-event)
                                            (= (:id node3-data) (-> v :node-id)))
                                      (deliver *expecting-event v))))
              *expecting-event3 (promise)
              event-catcher-fn3 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :upsert-neighbour)
                                            (= :alive (-> v :data :neighbour-node :status))
                                            (= (:id node1-data) (-> v :data :neighbour-node :id)))
                                      (deliver *expecting-event3 v))))
              *expecting-event4 (promise)
              event-catcher-fn4 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :put-event)
                                            (= 3 (-> v :data :event :cmd-type)))
                                      (deliver *expecting-event4 v))))
              *expecting-event5 (promise)
              event-catcher-fn5 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :udp-packet-processor)
                                            (= #uuid "00000000-0000-0000-0000-000000000001"
                                              (:node-id v))
                                            (= 3 (-> v :data :event :cmd-type)))
                                      (deliver *expecting-event5 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)
          (add-tap event-catcher-fn3)
          (add-tap event-catcher-fn4)
          (add-tap event-catcher-fn5)

          (sut/join node1)

          (testing "Join Event should happen on node2 or on node3"
            (no-timeout-check *expecting-event))

          (no-timeout-check *expecting-event3)
          (no-timeout-check *expecting-event4)
          (no-timeout-check *expecting-event5)

          (testing "node2 or node3 should upsert new alive node1 into neighbours map"
            (m/assert ^:matcho/strict
              {:node-id            uuid?
               :org.rssys.swim/cmd :upsert-neighbour
               :data
               {:neighbour-node
                {:access          :direct
                 :host            "127.0.0.1"
                 :id              #uuid "00000000-0000-0000-0000-000000000001"
                 :payload         {}
                 :port            5376
                 :restart-counter 0
                 :status          :alive
                 :tx              1
                 :updated-at      pos-int?}}
               :ts                 pos-int?}
              @*expecting-event3))

          (testing "node2 or node3 should update tx field for node1 in neighbours map"
            (let [node (if (= (:id node2-data) (:node-id @*expecting-event3))
                         node2
                         node3)]
              (m/assert
                pos-int?
                (:tx (sut/get-neighbour node (:id node1-data))))))

          (testing "node1 should get alive event from node2 or node3"
            (m/assert ^:matcho/strict
              {:data
               {:event
                {:cmd-type                  3
                 :id                        uuid?
                 :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                 :neighbour-restart-counter 8
                 :neighbour-tx              1
                 :restart-counter           pos-int?
                 :tx                        2}}
               :node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :ts                 pos-int?
               :org.rssys.swim/cmd :udp-packet-processor}
              @*expecting-event5))


          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2)
          (remove-tap event-catcher-fn3)
          (remove-tap event-catcher-fn4)
          (remove-tap event-catcher-fn5))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Do not process join event on not alive node"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :left)
        (sut/set-status node2 :dead)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc node1-nb-data :status :left)))

        (let [*expecting-event (promise)
              event-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (and
                                           (= cmd :join-event-not-alive-node-error)
                                           (= (:id node2-data) (-> v :node-id)))
                                     (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          (sut/join node1)

          (testing "Join Event should not happen on not alive node2"
            (no-timeout-check *expecting-event))

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Do not process join event from the dead node"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :left)
        (sut/set-status node2 :alive)
        (sut/set-status node3 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc node1-nb-data :restart-counter 999)))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node (assoc node1-nb-data :restart-counter 999)))

        (let [*expecting-event  (promise)
              event-catcher-fn  (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (= cmd :join-event-bad-restart-counter-error)
                                      (deliver *expecting-event v))))
              *expecting-event2 (promise)
              event-catcher-fn2 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :udp-packet-processor)
                                            (= 6 (-> v :data :event :cmd-type)))
                                      (deliver *expecting-event2 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)

          (sut/join node1)

          (no-timeout-check *expecting-event)

          (no-timeout-check *expecting-event2)

          (testing "node2 or node3 should not process join event with outdated restart counter"
            (m/assert ^:matcho/strict
              {:data
               {:cmd-type        2
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :port            5376
                :restart-counter 8
                :tx              1}
               :node-id            uuid?
               :ts                 pos-int?
               :org.rssys.swim/cmd :join-event-bad-restart-counter-error}
              @*expecting-event))

          (testing "node2 or node3 should send dead event to node with outdated restart counter"
            (m/assert ^:matcho/strict
              {:data
               {:event
                {:cmd-type                  6
                 :id                        uuid?
                 :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                 :neighbour-restart-counter 8
                 :neighbour-tx              1
                 :restart-counter           pos-int?
                 :tx                        2}}
               :node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :ts                 pos-int?
               :org.rssys.swim/cmd :udp-packet-processor}
              @*expecting-event2))

          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Do not process join event if cluster size exceeded and new node is not in the table of known nodes"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :left)
        (sut/set-status node2 :alive)
        (sut/set-status node3 :alive)

        (sut/set-cluster-size node2 2)
        (sut/set-cluster-size node3 2)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

        (let [*expecting-event  (promise)
              event-catcher-fn  (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (= cmd :join-event-cluster-size-exceeded-error)
                                      (deliver *expecting-event v))))
              *expecting-event2 (promise)
              event-catcher-fn2 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :udp-packet-processor)
                                            (= 6 (-> v :data :event :cmd-type)))
                                      (deliver *expecting-event2 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)

          (sut/join node1)

          (no-timeout-check *expecting-event)

          (no-timeout-check *expecting-event2)

          (testing "node2 or node3 should not accept join event if cluster size exceeded"
            (m/assert ^:matcho/strict
              {:data
               {:cmd-type        2
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :port            5376
                :restart-counter 8
                :tx              1}
               :node-id            uuid?
               :ts                 pos-int?
               :org.rssys.swim/cmd :join-event-cluster-size-exceeded-error}
              @*expecting-event))

          (testing "node2 or node3 should send dead event to a new node if cluster size exceeded"
            (m/assert ^:matcho/strict
              {:data
               {:event
                {:cmd-type                  6
                 :id                        uuid?
                 :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                 :neighbour-restart-counter 8
                 :neighbour-tx              1
                 :restart-counter           pos-int?
                 :tx                        2}}
               :node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :ts                 pos-int?
               :org.rssys.swim/cmd :udp-packet-processor}
              @*expecting-event2))

          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Do not process join event with outdated tx"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :left)
        (sut/set-status node2 :alive)
        (sut/set-status node3 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node (assoc node1-nb-data :tx 999)))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node (assoc node1-nb-data :tx 999)))

        (let [*expecting-event (promise)
              event-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (= cmd :join-event-bad-tx-error)
                                     (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          (sut/join node1)

          (no-timeout-check *expecting-event)

          (testing "node2 or node3 should not process join event with outdated tx"
            (m/assert ^:matcho/strict
              {:data
               {:cmd-type        2
                :host            "127.0.0.1"
                :id              #uuid "00000000-0000-0000-0000-000000000001"
                :port            5376
                :restart-counter 8
                :tx              1}
               :node-id            uuid?
               :ts                 pos-int?
               :org.rssys.swim/cmd :join-event-bad-tx-error}
              @*expecting-event))

          (remove-tap event-catcher-fn))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3))))))


(deftest ^:logic alive-event-test

  (testing "Process alive event on joining node1 for join confirmation from alive nodes"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :left)
        (sut/set-status node2 :alive)
        (sut/set-status node3 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

        (let [*expecting-event  (promise)
              event-catcher-fn  (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :alive-event-join-confirmed)
                                            (= (:id node1-data) (-> v :node-id)))
                                      (deliver *expecting-event v))))

              *expecting-event2 (promise)
              event-catcher-fn2 (fn [v]
                                  (when-let [cmd (:org.rssys.swim/cmd v)]
                                    (when (and
                                            (= cmd :set-status)
                                            (= :join (-> v :data :old-status))
                                            (= (:id node1-data) (-> v :node-id)))
                                      (deliver *expecting-event2 v))))]

          (add-tap event-catcher-fn)
          (add-tap event-catcher-fn2)

          (sut/join node1)

          (no-timeout-check *expecting-event)
          (no-timeout-check *expecting-event2)

          (testing "node1 should receive join event confirmation from any alive node in a cluster"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :org.rssys.swim/cmd :alive-event-join-confirmed
               :data
               {:cmd-type                  3
                :id                        uuid?
                :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                :neighbour-restart-counter 8
                :neighbour-tx              1
                :restart-counter           pos-int?
                :tx                        pos-int?}

               :ts                 pos-int?}
              @*expecting-event))

          (testing "node1 should set alive status after join confirmation"
            (m/assert ^:matcho/strict
              {:node-id            #uuid "00000000-0000-0000-0000-000000000001"
               :org.rssys.swim/cmd :set-status
               :data               {:new-status :alive, :old-status :join}
               :ts                 pos-int?}
              @*expecting-event2))


          (remove-tap event-catcher-fn)
          (remove-tap event-catcher-fn2))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3)))))

  (testing "Process alive event about joined node1 on alive nodes"
    (let [node1 (sut/new-node-object node1-data cluster)
          node2 (sut/new-node-object node2-data cluster)
          node3 (sut/new-node-object node3-data cluster)]
      (try

        (sut/start node1 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node2 empty-node-process-fn sut/udp-packet-processor)
        (sut/start node3 empty-node-process-fn sut/udp-packet-processor)

        (sut/set-status node1 :left)
        (sut/set-status node2 :alive)
        (sut/set-status node3 :alive)

        (sut/upsert-neighbour node1 (sut/new-neighbour-node node2-nb-data))
        (sut/upsert-neighbour node1 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node2 (sut/new-neighbour-node node3-nb-data))
        (sut/upsert-neighbour node3 (sut/new-neighbour-node node2-nb-data))

        (let [*expecting-event (promise)
              event-catcher-fn (fn [v]
                                 (when-let [cmd (:org.rssys.swim/cmd v)]
                                   (when (and
                                           (= cmd :put-event)
                                           (= 3 (-> v :data :event :cmd-type))
                                           (or
                                             (= (:id node2-data) (-> v :node-id))
                                             (= (:id node3-data) (-> v :node-id))))
                                     (deliver *expecting-event v))))]

          (add-tap event-catcher-fn)

          (sut/set-outgoing-events node2 [])
          (sut/set-outgoing-events node3 [])

          (sut/join node1)

          (no-timeout-check *expecting-event)

          (testing "node2 or node 3 should put alive event about node1 to outgoing buffer"
            (m/assert ^:matcho/strict
              {:data
               {:event
                {:cmd-type                  3               ;; alive
                 :id                        (fn [v]
                                              (or
                                                (= v (:id node2-data))
                                                (= v (:id node3-data))))

                 :neighbour-id              (:id node1-data)
                 :neighbour-restart-counter 8
                 :neighbour-tx              1
                 :restart-counter           pos-int?
                 :tx                        2}
                :tx pos-int?}
               :node-id            (fn [v]
                                     (or
                                       (= v (:id node2-data))
                                       (= v (:id node3-data))))
               :ts                 pos-int?
               :org.rssys.swim/cmd :put-event}
              @*expecting-event))


          (let [*expecting-event2 (promise)
                event-catcher-fn2 (fn [v]
                                    (when-let [cmd (:org.rssys.swim/cmd v)]
                                      (when (and
                                              (= cmd :alive-event)
                                              (or
                                                (= (:id node2-data) (-> v :node-id))
                                                (= (:id node3-data) (-> v :node-id))))
                                        (deliver *expecting-event2 v))))

                _                 (add-tap event-catcher-fn2)

                [node events host port]
                (condp = (:node-id @*expecting-event)

                  (:id node2-data)
                  [node2 (sut/take-events node2) (sut/get-host node3) (sut/get-port node3)]

                  (:id node3-data)
                  [node3 (sut/take-events node3) (sut/get-host node2) (sut/get-port node2)])]
            (sut/send-events node events host port)

            (no-timeout-check *expecting-event2)

            (remove-tap event-catcher-fn)
            (remove-tap event-catcher-fn2)))

        (catch Exception e
          (println (ex-message e)))
        (finally
          (sut/stop node1)
          (sut/stop node2)
          (sut/stop node3))))))






