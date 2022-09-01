(ns org.rssys.event-test
  (:require [clojure.test :refer [deftest is testing]]
            [matcho.core :refer [match]]
            [org.rssys.event :as sut])
  (:import (org.rssys.event PingEvent AckEvent DeadEvent)))


(deftest map->PingEvent-test

  (testing "PingEvent create"
    (let [ping1  ^PingEvent (sut/map->PingEvent {:cmd-type        0
                                                 :id              #uuid "00000000-0000-0000-0000-000000000001"
                                                 :host            "127.0.0.1"
                                                 :port            5376
                                                 :restart-counter 7
                                                 :tx              0
                                                 :neighbour-id    #uuid "8acc376e-f90d-470b-aa58-400a339d9424"
                                                 :attempt-number  42})
          result (.prepare ping1)]

      (testing "Prepare PingEvent to vector"
        (match result [(:ping sut/code)
                       (.-id ping1)
                       (.-host ping1)
                       (.-port ping1)
                       (.-restart_counter ping1)
                       (.-tx ping1)
                       (.-neighbour_id ping1)
                       (.-attempt_number ping1)]))

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

(deftest empty-ping-test
  (match (sut/empty-ping) {:cmd-type       (:ping sut/code)
                           :id             uuid?
                           :host           string?
                           :port           nat-int?
                           :tx             nat-int?
                           :neighbour-id   uuid?
                           :attempt-number pos-int?}))

;;;;


(deftest map->AckEvent-test

  (testing "AckEvent"
    (let [ping1  ^PingEvent (sut/map->PingEvent {:cmd-type        0
                                                 :id              #uuid "00000000-0000-0000-0000-000000000001"
                                                 :host            "127.0.0.1"
                                                 :port            5376
                                                 :restart-counter 7
                                                 :tx              0
                                                 :neighbour-id    #uuid "00000000-0000-0000-0000-000000000002"
                                                 :attempt-number  42})
          ack1   ^AckEvent (sut/map->AckEvent {:cmd-type        1
                                               :id              #uuid "00000000-0000-0000-0000-000000000002"
                                               :restart-counter 5
                                               :tx              0
                                               :neighbour-id    #uuid "00000000-0000-0000-0000-000000000001"
                                               :neighbour-tx    0
                                               })
          result (.prepare ack1)]

      (testing "Prepare AckEvent to vector"
        (match result [(:ack sut/code) (.id ack1) (.restart_counter ack1) (.tx ack1) (.id ping1) (.tx ping1)]))

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
    (let [dead1  ^DeadEvent (sut/map->DeadEvent {:cmd-type        (:dead sut/code)
                                                 :id              #uuid "00000000-0000-0000-0000-000000000002"
                                                 :restart-counter 5
                                                 :tx              0
                                                 :neighbour-id    #uuid "00000000-0000-0000-0000-000000000001"
                                                 :neighbour-tx    0})
          result (.prepare dead1)]

      (testing "Prepare DeadEvent to vector"
        (match result [(:dead sut/code) (.-id dead1) (.-restart_counter dead1) (.-tx dead1) (.-neighbour_id dead1) (.-neighbour_tx dead1)]))

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


(deftest empty-dead-test
  (match (sut/empty-dead) {:cmd-type        (:dead sut/code)
                           :id              uuid?
                           :restart-counter nat-int?
                           :tx              nat-int?
                           :neighbour-id    uuid?
                           :neighbour-tx    nat-int?}))

;;;;


