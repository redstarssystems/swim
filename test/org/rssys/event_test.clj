(ns org.rssys.event-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [matcho.core :as m :refer [match]]
    [org.rssys.event :as sut]
    [org.rssys.spec :as spec])
  (:import
    (org.rssys.event
      AckEvent
      AliveEvent
      AntiEntropy
      DeadEvent
      NewClusterSizeEvent
      PingEvent
      ProbeAckEvent
      ProbeEvent)))


;;;;

(deftest map->ProbeEvent-test
  (testing "ProbeEvent"
    (let [probe-event (sut/map->ProbeEvent {:cmd-type        9
                                            :id              #uuid "00000000-0000-0000-0000-000000000001"
                                            :host            "127.0.0.1"
                                            :port            5376
                                            :restart-counter 7
                                            :tx              1
                                            :neighbour-host  "127.0.0.1"
                                            :neighbour-port  5377
                                            :probe-key       "probe1"})]

      (testing "Prepare ProbeEvent to vector"
        (let [prepared-event (.prepare probe-event)]
          (m/assert ^:matcho/strict [(:probe sut/code)
                                     (.-id probe-event)
                                     (.-host probe-event)
                                     (.-port probe-event)
                                     (.-restart_counter probe-event)
                                     (.-tx probe-event)
                                     (.-neighbour_host probe-event)
                                     (.-neighbour_port probe-event)
                                     (.-probe_key probe-event)]
            prepared-event)))

      (testing "Restore ProbeEvent from vector"
        (let [v            [9
                            #uuid "00000000-0000-0000-0000-000000000001"
                            "127.0.0.1"
                            5376
                            7
                            1
                            "127.0.0.1"
                            5377
                            "probe1"]
              result-probe (.restore (sut/empty-probe) v)]

          (testing "Restored ProbeEvent should be equals to original event"
            (m/assert ProbeEvent (type probe-event))
            (m/assert probe-event result-probe))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"ProbeEvent vector has invalid structure"
              (.restore (sut/empty-probe) [])))
        (is (thrown-with-msg? Exception #"ProbeEvent vector has invalid structure"
              (.restore (sut/empty-probe) [999
                                           #uuid "00000000-0000-0000-0000-000000000001"
                                           "127.0.0.1"
                                           5376
                                           7
                                           1
                                           "127.0.0.1"
                                           5377
                                           "probe1"])))))))


(deftest empty-probe-test
  (m/assert ^:matcho/strict {:cmd-type        (:probe sut/code)
                             :id              uuid?
                             :host            string?
                             :port            nat-int?
                             :restart-counter nat-int?
                             :tx              nat-int?
                             :neighbour-host  string?
                             :neighbour-port  nat-int?
                             :probe-key       any?}
    (sut/empty-probe)))


;;;;

(deftest map->ProbeAckEvent-test
  (testing "ProbeAckEvent"
    (let [probe-ack-event
          (sut/map->ProbeAckEvent {:cmd-type        10
                                   :id              #uuid "00000000-0000-0000-0000-000000000001"
                                   :host            "127.0.0.1"
                                   :port            5376
                                   :status          :alive
                                   :restart-counter 7
                                   :tx              1
                                   :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                                   :probe-key       "probe1"})]

      (testing "Prepare ProbeAckEvent to vector"
        (let [prepared-event (.prepare probe-ack-event)]
          (m/assert ^:matcho/strict [(:probe-ack sut/code)
                                     (.-id probe-ack-event)
                                     (.-host probe-ack-event)
                                     (.-port probe-ack-event)
                                     3
                                     (.-restart_counter probe-ack-event)
                                     (.-tx probe-ack-event)
                                     (.-neighbour_id probe-ack-event)
                                     (.-probe_key probe-ack-event)]
            prepared-event)))

      (testing "Restore ProbeAckEvent from vector"
        (let [v            [10
                            #uuid "00000000-0000-0000-0000-000000000001"
                            "127.0.0.1"
                            5376
                            3
                            7
                            1
                            #uuid "00000000-0000-0000-0000-000000000002"
                            "probe1"]
              result-probe (.restore (sut/empty-probe-ack) v)]

          (testing "Restored ProbeAckEvent should be equals to original event"
            (m/assert ProbeAckEvent (type probe-ack-event))
            (m/assert probe-ack-event result-probe))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"ProbeAckEvent vector has invalid structure"
              (.restore (sut/empty-probe-ack) [])))
        (is (thrown-with-msg? Exception #"ProbeAckEvent vector has invalid structure"
              (.restore (sut/empty-probe-ack) [999
                                               #uuid "00000000-0000-0000-0000-000000000001"
                                               "127.0.0.1"
                                               5376
                                               3
                                               7
                                               1
                                               #uuid "00000000-0000-0000-0000-000000000002"
                                               "probe1"])))))))


(deftest empty-probe-ack-test
  (m/assert ^:matcho/strict {:cmd-type        (:probe-ack sut/code)
                             :id              uuid?
                             :host            string?
                             :port            nat-int?
                             :status          :unknown
                             :restart-counter nat-int?
                             :tx              nat-int?
                             :neighbour-id    uuid?
                             :probe-key       any?}
    (sut/empty-probe-ack)))

;;;;


(deftest map->PingEvent-test
  (testing "PingEvent"
    (let [ping1 (sut/map->PingEvent {:cmd-type        0
                                     :id              #uuid "00000000-0000-0000-0000-000000000001"
                                     :host            "127.0.0.1"
                                     :port            5376
                                     :restart-counter 7
                                     :tx              0
                                     :neighbour-id    #uuid "8acc376e-f90d-470b-aa58-400a339d9424"
                                     :attempt-number  42
                                     :ptype           :direct})]

      (testing "Prepare PingEvent to vector"
        (let [prepared-event (.prepare ping1)]
          (match prepared-event [(:ping sut/code)
                                 (.-id ping1)
                                 (.-host ping1)
                                 (.-port ping1)
                                 (.-restart_counter ping1)
                                 (.-tx ping1)
                                 (.-neighbour_id ping1)
                                 (.-attempt_number ping1)
                                 0])))

      (testing "Restore PingEvent from vector"
        (let [v           [0
                           #uuid "00000000-0000-0000-0000-000000000001"
                           "127.0.0.1"
                           5376
                           7
                           0
                           #uuid "8acc376e-f90d-470b-aa58-400a339d9424"
                           42
                           0]
              result-ping (.restore (sut/empty-ping) v)]

          (is (= PingEvent (type result-ping)) "Should be PingEvent type")
          (is (= result-ping ping1) "Restored PingEvent should be equals to original event")

          (testing "Wrong event structure is prohibited"
            (is (thrown-with-msg? Exception #"PingEvent vector has invalid structure"
                  (.restore (sut/empty-ping) [])))
            (is (thrown-with-msg? Exception #"PingEvent vector has invalid structure"
                  (.restore (sut/empty-ping) [999
                                              #uuid "00000000-0000-0000-0000-000000000001"
                                              "127.0.0.1"
                                              5376
                                              7
                                              0
                                              #uuid "8acc376e-f90d-470b-aa58-400a339d9424"
                                              42
                                              0])))))))))


(deftest empty-ping-test
  (match (sut/empty-ping) {:cmd-type       (:ping sut/code)
                           :id             uuid?
                           :host           string?
                           :port           nat-int?
                           :tx             nat-int?
                           :neighbour-id   uuid?
                           :attempt-number pos-int?
                           :ptype          keyword?}))


;;;;


(deftest map->AckEvent-test
  (testing "AckEvent"
    (let [ping1 (sut/map->PingEvent {:cmd-type        0
                                     :id              #uuid "00000000-0000-0000-0000-000000000001"
                                     :host            "127.0.0.1"
                                     :port            5376
                                     :restart-counter 7
                                     :tx              0
                                     :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                                     :attempt-number  42})
          ack1  (sut/map->AckEvent {:cmd-type        1
                                    :id              #uuid "00000000-0000-0000-0000-000000000002"
                                    :restart-counter 5
                                    :tx              0
                                    :neighbour-id    #uuid "00000000-0000-0000-0000-000000000001"
                                    :neighbour-tx    0})]

      (testing "Prepare AckEvent to vector"
        (let [prepared-event (.prepare ack1)]
          (match prepared-event [(:ack sut/code)
                                 (.id ack1)
                                 (.restart_counter ack1)
                                 (.tx ack1)
                                 (.id ping1)
                                 (.tx ping1)])))

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


          (testing "Wrong event structure is prohibited"
            (is (thrown-with-msg? Exception #"AckEvent vector has invalid structure"
                  (.restore (sut/empty-ack) [])))
            (is (thrown-with-msg? Exception #"AckEvent vector has invalid structure"
                  (.restore (sut/empty-ack) [999
                                             #uuid "00000000-0000-0000-0000-000000000002"
                                             5
                                             0
                                             #uuid "00000000-0000-0000-0000-000000000001"
                                             1])))))))))


(deftest empty-ack-test
  (match (sut/empty-ack) {:cmd-type        (:ack sut/code)
                          :id              uuid?
                          :restart-counter nat-int?
                          :tx              nat-int?
                          :neighbour-id    uuid?
                          :neighbour-tx    nat-int?}))


;;;;

(deftest map->DeadEvent-test
  (testing "DeadEvent"
    (let [dead1 (sut/map->DeadEvent {:cmd-type        6
                                     :id              #uuid "00000000-0000-0000-0000-000000000002"
                                     :restart-counter 5
                                     :tx              0
                                     :neighbour-id    #uuid "00000000-0000-0000-0000-000000000001"
                                     :neighbour-tx    0})]

      (testing "Prepare DeadEvent to vector"
        (let [prepared-event (.prepare dead1)]
          (match prepared-event [(:dead sut/code)
                                 (.-id dead1)
                                 (.-restart_counter dead1)
                                 (.-tx dead1)
                                 (.-neighbour_id dead1)
                                 (.-neighbour_tx dead1)])))

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


          (testing "Wrong event structure is prohibited"
            (is (thrown-with-msg? Exception #"DeadEvent vector has invalid structure"
                  (.restore (sut/empty-dead) [])))
            (is (thrown-with-msg? Exception #"DeadEvent vector has invalid structure"
                  (.restore (sut/empty-dead) [999
                                              #uuid "00000000-0000-0000-0000-000000000002"
                                              5
                                              0
                                              #uuid "00000000-0000-0000-0000-000000000001"
                                              1])))))))))


(deftest empty-dead-test
  (match (sut/empty-dead) {:cmd-type        (:dead sut/code)
                           :id              uuid?
                           :restart-counter nat-int?
                           :tx              nat-int?
                           :neighbour-id    uuid?
                           :neighbour-tx    nat-int?}))



;;;;





;;;;


(deftest map->AntiEntropy-test
  (testing "AntiEntropy"
    (let [anti-entropy-event (sut/map->AntiEntropy {:cmd-type          8
                                                    :id                #uuid "00000000-0000-0000-0000-000000000001"
                                                    :restart-counter   1
                                                    :tx                2
                                                    :anti-entropy-data [{:id              #uuid "00000000-0000-0000-0000-000000000002"
                                                                         :host            "127.0.0.1"
                                                                         :port            5432
                                                                         :status          :alive
                                                                         :access          :direct
                                                                         :restart-counter 2
                                                                         :tx              2
                                                                         :payload         {}
                                                                         :updated-at      1661799880969}]})]

      (testing "Prepare AntiEntropy to vector"
        (let [prepared-event (.prepare anti-entropy-event)]
          (match prepared-event [(:anti-entropy sut/code)
                                 #uuid "00000000-0000-0000-0000-000000000001"
                                 1
                                 2
                                 [{:id              #uuid "00000000-0000-0000-0000-000000000002"
                                   :host            "127.0.0.1"
                                   :port            5432
                                   :status          :alive
                                   :access          :direct
                                   :restart-counter 2
                                   :tx              2
                                   :payload         {}
                                   :updated-at      1661799880969}]])))

      (testing "Restore AntiEntropy from vector"
        (let [v                         [8
                                         #uuid "00000000-0000-0000-0000-000000000001"
                                         1
                                         2
                                         [{:id              #uuid "00000000-0000-0000-0000-000000000002"
                                           :host            "127.0.0.1"
                                           :port            5432
                                           :status          :alive
                                           :access          :direct
                                           :restart-counter 2
                                           :tx              2
                                           :payload         {}
                                           :updated-at      1661799880969}]]
              result-anti-entropy-event (.restore (sut/empty-anti-entropy) v)]

          (is (= AntiEntropy (type result-anti-entropy-event)) "Should be AntiEntropy type")
          (is (= result-anti-entropy-event anti-entropy-event) "Restored AntiEntropy should be equals to original event")))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"AntiEntropy vector has invalid structure"
              (.restore (sut/empty-anti-entropy) [])))
        (is (thrown-with-msg? Exception #"AntiEntropy vector has invalid structure"
              (.restore (sut/empty-anti-entropy) [999
                                                  #uuid "00000000-0000-0000-0000-000000000001"
                                                  1
                                                  2
                                                  [{:id              #uuid "00000000-0000-0000-0000-000000000002"
                                                    :host            "127.0.0.1"
                                                    :port            5432
                                                    :status          :alive
                                                    :access          :direct
                                                    :restart-counter 2
                                                    :tx              2
                                                    :payload         {}
                                                    :updated-at      1661799880969}]])))))))



(deftest empty-anti-entropy-test
  (match (sut/empty-anti-entropy) {:cmd-type          8
                                   :id                #uuid "00000000-0000-0000-0000-000000000000"
                                   :restart-counter   0
                                   :tx                0
                                   :anti-entropy-data []}))


;;;;

(deftest map->AliveEvent-test
  (testing "AliveEvent"
    (let [alive-event (sut/map->AliveEvent {:cmd-type        3
                                            :id              #uuid "00000000-0000-0000-0000-000000000002"
                                            :restart-counter 5
                                            :tx              0
                                            :neighbour-id    #uuid "00000000-0000-0000-0000-000000000001"
                                            :neighbour-tx    42})]

      (testing "Prepare AliveEvent to vector"
        (let [prepared-event (.prepare alive-event)]
          (match prepared-event [(:alive sut/code)
                                 (.id alive-event)
                                 (.restart_counter alive-event)
                                 (.tx alive-event)
                                 #uuid "00000000-0000-0000-0000-000000000001"
                                 42])))

      (testing "Restore AliveEvent from vector"
        (let [v            [3
                            #uuid "00000000-0000-0000-0000-000000000002"
                            5
                            0
                            #uuid "00000000-0000-0000-0000-000000000001"
                            42]
              result-alive (.restore (sut/empty-alive) v)]

          (is (= AliveEvent (type result-alive)) "Should be AliveEvent type")
          (is (= result-alive alive-event) "Restored AliveEvent should be equals to original event")


          (testing "Wrong event structure is prohibited"
            (is (thrown-with-msg? Exception #"AliveEvent vector has invalid structure"
                  (.restore (sut/empty-alive) [])))
            (is (thrown-with-msg? Exception #"AliveEvent vector has invalid structure"
                  (.restore (sut/empty-alive) [999
                                               #uuid "00000000-0000-0000-0000-000000000002"
                                               5
                                               0
                                               #uuid "00000000-0000-0000-0000-000000000001"
                                               42])))))))))


(deftest empty-alive-test
  (match (sut/empty-alive) {:cmd-type        (:alive sut/code)
                            :id              uuid?
                            :restart-counter nat-int?
                            :tx              nat-int?
                            :neighbour-id    uuid?
                            :neighbour-tx    nat-int?}))


;;;;

;;;;

(deftest map->NewClusterSizeEvent-test
  (testing "NewClusterSizeEvent"
    (let [ncs-event (sut/map->NewClusterSizeEvent {:cmd-type         13
                                                   :id               #uuid "00000000-0000-0000-0000-000000000002"
                                                   :restart-counter  5
                                                   :tx               0
                                                   :old-cluster-size 1
                                                   :new-cluster-size 3})]

      (testing "Prepare NewClusterSizeEvent to vector"
        (let [prepared-event (.prepare ncs-event)]
          (match prepared-event [(:new-cluster-size sut/code)
                                 (.id ncs-event)
                                 (.restart_counter ncs-event)
                                 (.tx ncs-event)
                                 1
                                 3])))

      (testing "Restore NewClusterSizeEvent from vector"
        (let [v            [13
                            #uuid "00000000-0000-0000-0000-000000000002"
                            5
                            0
                            1
                            3]
              result-event (.restore (sut/empty-new-cluster-size) v)]

          (is (= NewClusterSizeEvent (type result-event)) "Should be NewClusterSizeEvent type")
          (is (= result-event ncs-event) "Restored NewClusterSizeEvent should be equals to original event")


          (testing "Wrong event structure is prohibited"
            (is (thrown-with-msg? Exception #"NewClusterSizeEvent vector has invalid structure"
                  (.restore (sut/empty-new-cluster-size) [])))
            (is (thrown-with-msg? Exception #"NewClusterSizeEvent vector has invalid structure"
                  (.restore (sut/empty-new-cluster-size) [999
                                                          #uuid "00000000-0000-0000-0000-000000000002"
                                                          5
                                                          0
                                                          1
                                                          3])))))))))


(deftest empty-new-cluster-size-test
  (match (sut/empty-new-cluster-size) {:cmd-type         (:new-cluster-size sut/code)
                                       :id               uuid?
                                       :restart-counter  nat-int?
                                       :tx               nat-int?
                                       :old-cluster-size nat-int?
                                       :new-cluster-size nat-int?}))
