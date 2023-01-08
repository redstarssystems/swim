(ns org.rssys.event-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [matcho.core :as m :refer [match]]
    [org.rssys.event :as sut])
  (:import
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
      SuspectEvent)))


(declare thrown-with-msg?)


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
                             :probe-key       uuid?}
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
                                     (.-status probe-ack-event)
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
                            :alive
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
    (let [ping-event
          (sut/map->PingEvent {:cmd-type        0
                               :id              #uuid "00000000-0000-0000-0000-000000000001"
                               :host            "127.0.0.1"
                               :port            5376
                               :restart-counter 7
                               :tx              0
                               :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                               :attempt-number  42
                               :ts              1})]

      (testing "Prepare PingEvent to vector"
        (let [prepared-event (.prepare ping-event)]
          (m/assert ^:matcho/strict [(:ping sut/code)
                                     (.-id ping-event)
                                     (.-host ping-event)
                                     (.-port ping-event)
                                     (.-restart_counter ping-event)
                                     (.-tx ping-event)
                                     (.-neighbour_id ping-event)
                                     (.-attempt_number ping-event)
                                     (.-ts ping-event)]
            prepared-event)))

      (testing "Restore PingEvent from vector"
        (let [v           [0
                           #uuid "00000000-0000-0000-0000-000000000001"
                           "127.0.0.1"
                           5376
                           7
                           0
                           #uuid "00000000-0000-0000-0000-000000000002"
                           42
                           1]
              result-ping (.restore (sut/empty-ping) v)]

          (testing "Restored PingEvent should be equals to original event"
            (m/assert PingEvent (type result-ping))
            (m/assert ping-event result-ping))))

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
                                          0])))))))


(deftest empty-ping-test
  (m/assert ^:matcho/strict {:cmd-type        (:ping sut/code)
                             :id              uuid?
                             :host            string?
                             :port            nat-int?
                             :restart-counter nat-int?
                             :tx              nat-int?
                             :neighbour-id    uuid?
                             :attempt-number  pos-int?
                             :ts              pos-int?}
    (sut/empty-ping)))


;;;;


(deftest map->AckEvent-test
  (testing "AckEvent"
    (let [ack-event (sut/map->AckEvent {:cmd-type        1
                                        :id              #uuid "00000000-0000-0000-0000-000000000002"
                                        :restart-counter 5
                                        :tx              2
                                        :neighbour-id    #uuid "00000000-0000-0000-0000-000000000001"
                                        :neighbour-tx    3
                                        :attempt-number  42})]

      (testing "Prepare AckEvent to vector"
        (let [prepared-event (.prepare ack-event)]
          (m/assert ^:matcho/strict [(:ack sut/code)
                                     (.-id ack-event)
                                     (.-restart_counter ack-event)
                                     (.-tx ack-event)
                                     (.-neighbour_id ack-event)
                                     (.-neighbour_tx ack-event)
                                     (.-attempt_number ack-event)]
            prepared-event)))

      (testing "Restore AckEvent from vector"
        (let [v          [1
                          #uuid "00000000-0000-0000-0000-000000000002"
                          5
                          2
                          #uuid "00000000-0000-0000-0000-000000000001"
                          3
                          42]
              result-ack (.restore (sut/empty-ack) v)]

          (testing "Restored AckEvent should be equals to original event"
            (m/assert AckEvent (type result-ack))
            (m/assert ack-event result-ack))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"AckEvent vector has invalid structure"
              (.restore (sut/empty-ack) [])))
        (is (thrown-with-msg? Exception #"AckEvent vector has invalid structure"
              (.restore (sut/empty-ack) [999
                                         #uuid "00000000-0000-0000-0000-000000000002"
                                         5
                                         2
                                         #uuid "00000000-0000-0000-0000-000000000001"
                                         3
                                         42])))))))


(deftest empty-ack-test
  (m/assert ^:matcho/strict {:cmd-type        (:ack sut/code)
                             :id              uuid?
                             :restart-counter nat-int?
                             :tx              nat-int?
                             :neighbour-id    uuid?
                             :neighbour-tx    nat-int?
                             :attempt-number  pos-int?}
    (sut/empty-ack)))


;;;;


(deftest map->IndirectPingEvent-test
  (testing "IndirectPingEvent"
    (let [indirect-ping-event
          (sut/map->IndirectPingEvent {:cmd-type          14
                                       :id                #uuid "00000000-0000-0000-0000-000000000001"
                                       :host              "127.0.0.1"
                                       :port              5376
                                       :restart-counter   7
                                       :tx                0
                                       :intermediate-id   #uuid "00000000-0000-0000-0000-000000000002"
                                       :intermediate-host "127.0.0.1"
                                       :intermediate-port 5377
                                       :neighbour-id      #uuid "00000000-0000-0000-0000-000000000003"
                                       :neighbour-host    "127.0.0.1"
                                       :neighbour-port    5378
                                       :attempt-number    42
                                       :ts                1})]

      (testing "Prepare IndirectPingEvent to vector"
        (let [prepared-event (.prepare indirect-ping-event)]
          (m/assert ^:matcho/strict [(:indirect-ping sut/code)
                                     (.-id indirect-ping-event)
                                     (.-host indirect-ping-event)
                                     (.-port indirect-ping-event)
                                     (.-restart_counter indirect-ping-event)
                                     (.-tx indirect-ping-event)
                                     (.-intermediate_id indirect-ping-event)
                                     (.-intermediate_host indirect-ping-event)
                                     (.-intermediate_port indirect-ping-event)
                                     (.-neighbour_id indirect-ping-event)
                                     (.-neighbour_host indirect-ping-event)
                                     (.-neighbour_port indirect-ping-event)
                                     (.-attempt_number indirect-ping-event)
                                     (.-ts indirect-ping-event)]
            prepared-event)))

      (testing "Restore IndirectPingEvent from vector"
        (let [v [14
                 #uuid "00000000-0000-0000-0000-000000000001"
                 "127.0.0.1"
                 5376
                 7
                 0
                 #uuid "00000000-0000-0000-0000-000000000002"
                 "127.0.0.1"
                 5377 #uuid "00000000-0000-0000-0000-000000000003"
                 "127.0.0.1"
                 5378
                 42
                 1]
              result-indirect-ping
              (.restore (sut/empty-indirect-ping) v)]

          (testing "Restored IndirectPingEvent should be equals to original event"
            (m/assert IndirectPingEvent (type result-indirect-ping))
            (m/assert indirect-ping-event result-indirect-ping))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"IndirectPingEvent vector has invalid structure"
              (.restore (sut/empty-indirect-ping) [])))
        (is (thrown-with-msg? Exception #"IndirectPingEvent vector has invalid structure"
              (.restore (sut/empty-indirect-ping) [999
                                                   #uuid "00000000-0000-0000-0000-000000000001"
                                                   "127.0.0.1"
                                                   5376
                                                   7
                                                   0
                                                   #uuid "00000000-0000-0000-0000-000000000002"
                                                   "127.0.0.1"
                                                   5377
                                                   #uuid "00000000-0000-0000-0000-000000000003"
                                                   "127.0.0.1"
                                                   5378
                                                   42])))))))


(deftest empty-indirect-ping-test
  (m/assert ^:matcho/strict {:cmd-type          (:indirect-ping sut/code)
                             :id                uuid?
                             :host              string?
                             :port              nat-int?
                             :restart-counter   nat-int?
                             :tx                nat-int?
                             :intermediate-id   uuid?
                             :intermediate-host string?
                             :intermediate-port pos-int?
                             :neighbour-id      uuid?
                             :neighbour-host    string?
                             :neighbour-port    pos-int?
                             :attempt-number    pos-int?
                             :ts                pos-int?}
    (sut/empty-indirect-ping)))



;;;;


(deftest map->IndirectAckEvent-test
  (testing "IndirectAckEvent"
    (let [indirect-ack-event
          (sut/map->IndirectAckEvent {:cmd-type          15
                                      :id                #uuid "00000000-0000-0000-0000-000000000001"
                                      :host              "127.0.0.1"
                                      :port              5376
                                      :restart-counter   7
                                      :tx                0
                                      :status            :alive
                                      :intermediate-id   #uuid "00000000-0000-0000-0000-000000000002"
                                      :intermediate-host "127.0.0.1"
                                      :intermediate-port 5377
                                      :neighbour-id      #uuid "00000000-0000-0000-0000-000000000003"
                                      :neighbour-host    "127.0.0.1"
                                      :neighbour-port    5378
                                      :attempt-number    42})]

      (testing "Prepare IndirectAckEvent to vector"
        (let [prepared-event (.prepare indirect-ack-event)]
          (m/assert ^:matcho/strict [(:indirect-ack sut/code)
                                     (.-id indirect-ack-event)
                                     (.-host indirect-ack-event)
                                     (.-port indirect-ack-event)
                                     (.-restart_counter indirect-ack-event)
                                     (.-tx indirect-ack-event)
                                     (.-status indirect-ack-event)
                                     (.-intermediate_id indirect-ack-event)
                                     (.-intermediate_host indirect-ack-event)
                                     (.-intermediate_port indirect-ack-event)
                                     (.-neighbour_id indirect-ack-event)
                                     (.-neighbour_host indirect-ack-event)
                                     (.-neighbour_port indirect-ack-event)
                                     (.-attempt_number indirect-ack-event)]
            prepared-event)))

      (testing "Restore IndirectAckEvent from vector"
        (let [v [15
                 #uuid "00000000-0000-0000-0000-000000000001"
                 "127.0.0.1"
                 5376
                 7
                 0
                 :alive
                 #uuid "00000000-0000-0000-0000-000000000002"
                 "127.0.0.1"
                 5377
                 #uuid "00000000-0000-0000-0000-000000000003"
                 "127.0.0.1"
                 5378
                 42]
              result-indirect-ack
              (.restore (sut/empty-indirect-ack) v)]

          (testing "Restored IndirectAckEvent should be equals to original event"
            (m/assert IndirectAckEvent (type result-indirect-ack))
            (m/assert indirect-ack-event result-indirect-ack))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"IndirectAckEvent vector has invalid structure"
              (.restore (sut/empty-indirect-ack) [])))
        (is (thrown-with-msg? Exception #"IndirectAckEvent vector has invalid structure"
              (.restore (sut/empty-indirect-ack) [999
                                                  #uuid "00000000-0000-0000-0000-000000000001"
                                                  "127.0.0.1"
                                                  5376
                                                  7
                                                  0
                                                  (:alive sut/code)
                                                  #uuid "00000000-0000-0000-0000-000000000002"
                                                  "127.0.0.1"
                                                  5377
                                                  #uuid "00000000-0000-0000-0000-000000000003"
                                                  "127.0.0.1"
                                                  5378
                                                  42])))))))


(deftest empty-indirect-ack-test
  (m/assert ^:matcho/strict {:cmd-type          (:indirect-ack sut/code)
                             :id                uuid?
                             :host              string?
                             :port              nat-int?
                             :restart-counter   nat-int?
                             :tx                nat-int?
                             :status            :unknown
                             :intermediate-id   uuid?
                             :intermediate-host string?
                             :intermediate-port pos-int?
                             :neighbour-id      uuid?
                             :neighbour-host    string?
                             :neighbour-port    pos-int?
                             :attempt-number    pos-int?}
    (sut/empty-indirect-ack)))


;;;;

(deftest map->AliveEvent-test
  (testing "AliveEvent"
    (let [alive-event
          (sut/map->AliveEvent {:cmd-type                  3
                                :id                        #uuid "00000000-0000-0000-0000-000000000002"
                                :restart-counter           5
                                :tx                        0
                                :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                                :neighbour-restart-counter 7
                                :neighbour-tx              42
                                :neighbour-host            "localhost"
                                :neighbour-port            1})]

      (testing "Prepare AliveEvent to vector"
        (let [prepared-event (.prepare alive-event)]
          (m/assert ^:matcho/strict [(:alive sut/code)
                                     (.-id alive-event)
                                     (.-restart_counter alive-event)
                                     (.-tx alive-event)
                                     #uuid "00000000-0000-0000-0000-000000000001"
                                     7
                                     42
                                     "localhost"
                                     1]
            prepared-event)))

      (testing "Restore AliveEvent from vector"
        (let [v            [3
                            #uuid "00000000-0000-0000-0000-000000000002"
                            5
                            0
                            #uuid "00000000-0000-0000-0000-000000000001"
                            7
                            42
                            "localhost"
                            1]
              result-alive (.restore (sut/empty-alive) v)]

          (testing "Restored AliveEvent should be equals to original event"
            (m/assert AliveEvent (type result-alive))
            (m/assert alive-event result-alive))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"AliveEvent vector has invalid structure"
              (.restore (sut/empty-alive) [])))
        (is (thrown-with-msg? Exception #"AliveEvent vector has invalid structure"
              (.restore (sut/empty-alive) [999
                                           #uuid "00000000-0000-0000-0000-000000000002"
                                           5
                                           0
                                           #uuid "00000000-0000-0000-0000-000000000001"
                                           7
                                           42
                                           "localhost"
                                           1])))))))


(deftest empty-alive-test
  (m/assert ^:matcho/strict {:cmd-type                  (:alive sut/code)
                             :id                        uuid?
                             :restart-counter           nat-int?
                             :tx                        nat-int?
                             :neighbour-id              uuid?
                             :neighbour-restart-counter nat-int?
                             :neighbour-tx              nat-int?
                             :neighbour-host            string?
                             :neighbour-port            pos-int?}
    (sut/empty-alive)))


;;;;

(deftest map->NewClusterSizeEvent-test
  (testing "NewClusterSizeEvent"
    (let [ncs-event
          (sut/map->NewClusterSizeEvent {:cmd-type         13
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

          (testing "Restored NewClusterSizeEvent should be equals to original event"
            (m/assert NewClusterSizeEvent (type result-event))
            (m/assert ncs-event result-event))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"NewClusterSizeEvent vector has invalid structure"
              (.restore (sut/empty-new-cluster-size) [])))
        (is (thrown-with-msg? Exception #"NewClusterSizeEvent vector has invalid structure"
              (.restore (sut/empty-new-cluster-size) [999
                                                      #uuid "00000000-0000-0000-0000-000000000002"
                                                      5
                                                      0
                                                      1
                                                      3])))))))


(deftest empty-new-cluster-size-test
  (m/assert ^:matcho/strict {:cmd-type         (:new-cluster-size sut/code)
                             :id               uuid?
                             :restart-counter  nat-int?
                             :tx               nat-int?
                             :old-cluster-size nat-int?
                             :new-cluster-size nat-int?}
    (sut/empty-new-cluster-size)))


;;;;


(deftest map->DeadEvent-test
  (testing "DeadEvent"
    (let [dead-event
          (sut/map->DeadEvent {:cmd-type                  6
                               :id                        #uuid "00000000-0000-0000-0000-000000000002"
                               :restart-counter           5
                               :tx                        0
                               :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                               :neighbour-restart-counter 7
                               :neighbour-tx              1})]

      (testing "Prepare DeadEvent to vector"
        (let [prepared-event (.prepare dead-event)]
          (m/assert ^:matcho/strict [(:dead sut/code)
                                     (.-id dead-event)
                                     (.-restart_counter dead-event)
                                     (.-tx dead-event)
                                     (.-neighbour_id dead-event)
                                     (.-neighbour_restart_counter dead-event)
                                     (.-neighbour_tx dead-event)]
            prepared-event)))

      (testing "Restore DeadEvent from vector"
        (let [v           [6
                           #uuid "00000000-0000-0000-0000-000000000002"
                           5
                           0
                           #uuid "00000000-0000-0000-0000-000000000001"
                           7
                           1]
              result-dead (.restore (sut/empty-dead) v)]

          (testing "Restored DeadEvent should be equals to original event"
            (m/assert DeadEvent (type result-dead))
            (m/assert dead-event result-dead))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"DeadEvent vector has invalid structure"
              (.restore (sut/empty-dead) [])))
        (is (thrown-with-msg? Exception #"DeadEvent vector has invalid structure"
              (.restore (sut/empty-dead) [999
                                          #uuid "00000000-0000-0000-0000-000000000002"
                                          5
                                          0
                                          #uuid "00000000-0000-0000-0000-000000000001"
                                          7
                                          1])))))))


(deftest empty-dead-test
  (m/assert ^:matcho/strict {:cmd-type                  (:dead sut/code)
                             :id                        uuid?
                             :restart-counter           nat-int?
                             :tx                        nat-int?
                             :neighbour-id              uuid?
                             :neighbour-restart-counter nat-int?
                             :neighbour-tx              nat-int?}
    (sut/empty-dead)))



;;;;


(deftest map->AntiEntropy-test
  (testing "AntiEntropy"
    (let [anti-entropy-event
          (sut/map->AntiEntropy {:cmd-type          8
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
          (m/assert ^:matcho/strict [(:anti-entropy sut/code)
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
            prepared-event)))

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

          (testing "Restored AntiEntropy should be equals to original event"
            (m/assert AntiEntropy (type result-anti-entropy-event))
            (m/assert anti-entropy-event result-anti-entropy-event))))

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
  (m/assert ^:matcho/strict {:cmd-type          8
                             :id                #uuid "00000000-0000-0000-0000-000000000000"
                             :restart-counter   0
                             :tx                0
                             :anti-entropy-data []}
    (sut/empty-anti-entropy)))


;;;;

(deftest map->JoinEvent-test
  (testing "JoinEvent"
    (let [join-event
          (sut/map->JoinEvent {:cmd-type        2
                               :id              #uuid "00000000-0000-0000-0000-000000000001"
                               :restart-counter 5
                               :tx              1
                               :host            "localhost"
                               :port            5678})]

      (testing "Prepare JoinEvent to vector"
        (let [prepared-event (.prepare join-event)]
          (m/assert ^:matcho/strict [(:join sut/code)
                                     (.-id join-event)
                                     (.-restart_counter join-event)
                                     (.-tx join-event)
                                     (.-host join-event)
                                     (.-port join-event)]
            prepared-event)))

      (testing "Restore JoinEvent from vector"
        (let [v           [2
                           #uuid "00000000-0000-0000-0000-000000000001"
                           5
                           1
                           "localhost"
                           5678]
              result-join (.restore (sut/empty-join) v)]

          (testing "Restored JoinEvent should be equals to original event"
            (m/assert JoinEvent (type result-join))
            (m/assert join-event result-join))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"JoinEvent vector has invalid structure"
              (.restore (sut/empty-join) [])))
        (is (thrown-with-msg? Exception #"JoinEvent vector has invalid structure"
              (.restore (sut/empty-join) [999
                                          #uuid "00000000-0000-0000-0000-000000000002"
                                          5
                                          1
                                          "localhost"
                                          5678])))))))


(deftest empty-join-test
  (m/assert ^:matcho/strict {:cmd-type        (:join sut/code)
                             :id              uuid?
                             :restart-counter nat-int?
                             :tx              nat-int?
                             :host            string?
                             :port            nat-int?}
    (sut/empty-join)))


;;;;


(deftest map->SuspectEvent-test
  (testing "SuspectEvent"
    (let [suspect-event
          (sut/map->SuspectEvent {:cmd-type                  4
                                  :id                        #uuid "00000000-0000-0000-0000-000000000002"
                                  :restart-counter           5
                                  :tx                        0
                                  :neighbour-id              #uuid "00000000-0000-0000-0000-000000000001"
                                  :neighbour-restart-counter 7
                                  :neighbour-tx              1})]

      (testing "Prepare SuspectEvent to vector"
        (let [prepared-event (.prepare suspect-event)]
          (m/assert ^:matcho/strict [(:suspect sut/code)
                                     (.-id suspect-event)
                                     (.-restart_counter suspect-event)
                                     (.-tx suspect-event)
                                     (.-neighbour_id suspect-event)
                                     (.-neighbour_restart_counter suspect-event)
                                     (.-neighbour_tx suspect-event)]
            prepared-event)))

      (testing "Restore SuspectEvent from vector"
        (let [v           [4
                           #uuid "00000000-0000-0000-0000-000000000002"
                           5
                           0
                           #uuid "00000000-0000-0000-0000-000000000001"
                           7
                           1]
              result-dead (.restore (sut/empty-suspect) v)]

          (testing "Restored SuspectEvent should be equals to original event"
            (m/assert SuspectEvent (type result-dead))
            (m/assert suspect-event result-dead))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"SuspectEvent vector has invalid structure"
              (.restore (sut/empty-suspect) [])))
        (is (thrown-with-msg? Exception #"SuspectEvent vector has invalid structure"
              (.restore (sut/empty-suspect) [999
                                             #uuid "00000000-0000-0000-0000-000000000002"
                                             5
                                             0
                                             #uuid "00000000-0000-0000-0000-000000000001"
                                             7
                                             1])))))))


(deftest empty-suspect-test
  (m/assert ^:matcho/strict {:cmd-type                  (:suspect sut/code)
                             :id                        uuid?
                             :restart-counter           nat-int?
                             :tx                        nat-int?
                             :neighbour-id              uuid?
                             :neighbour-restart-counter nat-int?
                             :neighbour-tx              nat-int?}
    (sut/empty-suspect)))



;;;;

(deftest map->LeftEvent-test
  (testing "LeftEvent"
    (let [left-event
          (sut/map->LeftEvent {:cmd-type        5
                               :id              #uuid "00000000-0000-0000-0000-000000000001"
                               :restart-counter 7
                               :tx              1})]

      (testing "Prepare LeftEvent to vector"
        (let [prepared-event (.prepare left-event)]
          (m/assert ^:matcho/strict [(:left sut/code)
                                     (.-id left-event)
                                     (.-restart_counter left-event)
                                     (.-tx left-event)]
            prepared-event)))

      (testing "Restore LeftEvent from vector"
        (let [v           [5
                           #uuid "00000000-0000-0000-0000-000000000001"
                           7
                           1]
              result-left (.restore (sut/empty-left) v)]

          (testing "Restored LeftEvent should be equals to original event"
            (m/assert LeftEvent (type result-left))
            (m/assert left-event result-left))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"LeftEvent vector has invalid structure"
              (.restore (sut/empty-left) [])))
        (is (thrown-with-msg? Exception #"LeftEvent vector has invalid structure"
              (.restore (sut/empty-left) [999
                                          #uuid "00000000-0000-0000-0000-000000000002"
                                          7
                                          1])))))))


(deftest empty-left-test
  (m/assert ^:matcho/strict {:cmd-type        (:left sut/code)
                             :id              uuid?
                             :restart-counter nat-int?
                             :tx              nat-int?}
    (sut/empty-left)))


;;;;

(deftest map->PayloadEvent-test
  (testing "PayloadEvent"
    (let [payload-event
          (sut/map->PayloadEvent {:cmd-type        7
                                  :id              #uuid "00000000-0000-0000-0000-000000000001"
                                  :restart-counter 5
                                  :tx              1
                                  :payload         {:name "node1"}})]

      (testing "Prepare PayloadEvent to vector"
        (let [prepared-event (.prepare payload-event)]
          (m/assert ^:matcho/strict [(:payload sut/code)
                                     (.-id payload-event)
                                     (.-restart_counter payload-event)
                                     (.-tx payload-event)
                                     (.-payload payload-event)]
            prepared-event)))

      (testing "Restore PayloadEvent from vector"
        (let [v              [7
                              #uuid "00000000-0000-0000-0000-000000000001"
                              5
                              1
                              {:name "node1"}]
              result-payload (.restore (sut/empty-payload) v)]

          (testing "Restored PayloadEvent should be equals to original event"
            (m/assert PayloadEvent (type result-payload))
            (m/assert payload-event result-payload))))

      (testing "Wrong event structure is prohibited"
        (is (thrown-with-msg? Exception #"PayloadEvent vector has invalid structure"
              (.restore (sut/empty-payload) [])))
        (is (thrown-with-msg? Exception #"PayloadEvent vector has invalid structure"
              (.restore (sut/empty-payload) [999
                                             #uuid "00000000-0000-0000-0000-000000000002"
                                             5
                                             1])))))))


(deftest empty-payload-test
  (m/assert ^:matcho/strict {:cmd-type        (:payload sut/code)
                             :id              uuid?
                             :restart-counter nat-int?
                             :tx              nat-int?
                             :payload         any?}
    (sut/empty-payload)))


