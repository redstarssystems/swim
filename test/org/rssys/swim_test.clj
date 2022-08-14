(ns org.rssys.swim-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test :refer [deftest is testing]]
    [matcho.core :refer [match]]
    [org.rssys.swim :as sut])
  (:import
    (javax.crypto
      Cipher)
    (org.rssys.scheduler
      MutablePool)
    (org.rssys.swim
      Cluster
      NeighbourNode
      NodeObject)))


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
   :status          :normal
   :access          :direct
   :restart-counter 0
   :tx              1
   :payload         {:tcp-port 4567}})


(def neighbour-data2
  {:id              #uuid"00000000-0000-0000-0000-000000000003"
   :host            "127.0.0.1"
   :port            5378
   :status          :normal
   :access          :direct
   :restart-counter 0
   :tx              1
   :payload         {:tcp-port 4567}})


(def neighbour-data3
  {:id              #uuid"00000000-0000-0000-0000-000000000001"
   :host            "127.0.0.1"
   :port            5376
   :status          :normal
   :access          :direct
   :restart-counter 0
   :tx              1
   :payload         {:tcp-port 4567}})


(def node-data1 {:id #uuid "00000000-0000-0000-0000-000000000001" :host "127.0.0.1" :port 5376})
(def node-data2 {:id #uuid "00000000-0000-0000-0000-000000000002" :host "127.0.0.1" :port 5377})
(def node-data3 {:id #uuid "00000000-0000-0000-0000-000000000003" :host "127.0.0.1" :port 5378})


;;;;;;;;;;


(deftest new-cluster-test
  (testing "Create Cluster instance is successful"
    (let [result (sut/new-cluster cluster-data)]
      (is (instance? Cluster result) "Should be Cluster instance")
      (is (s/valid? ::sut/cluster result)))))


(deftest new-neighbour-node-test
  (testing "Create NeighbourNode instance is successful"
    (let [result1 (sut/new-neighbour-node neighbour-data1)
          result2 (sut/new-neighbour-node "127.0.0.1" 5379)]
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
                      :payload         {}}))))


(def cluster (sut/new-cluster cluster-data))


(deftest new-node-object-test

  (testing "Create NodeObject instance is successful"
    (let [result1 (sut/new-node-object {:id #uuid "00000000-0000-0000-0000-000000000001" :host "127.0.0.1" :port 5376} cluster)]
      (is (instance? NodeObject result1) "Should be NodeObject instance")
      (is (s/valid? ::sut/node (.value result1)))
      (match (.value result1)
        {:id              #uuid "00000000-0000-0000-0000-000000000001"
         :host            "127.0.0.1"
         :port            5376
         :cluster         cluster
         :status          :stopped
         :neighbours      {}
         :restart-counter 0
         :tx              0
         :ping-events     []
         :payload         {}
         :scheduler-pool  #(instance? MutablePool %)
         :*udp-server     nil})))

  (testing "Wrong data is caught by spec"
    (is (thrown-with-msg? Exception #"Invalid node data"
          (sut/new-node-object {:a 1} cluster)))))


(deftest map->NodeObject-test

  (testing "Getters should return expected values"
    (let [node-object (sut/new-node-object node-data1 cluster)
          result      (.value node-object)]

      (match result {:id              ::sut/id
                     :host            ::sut/host
                     :port            ::sut/port
                     :cluster         ::sut/cluster
                     :status          ::sut/status
                     :neighbours      ::sut/neighbours
                     :restart-counter ::sut/restart-counter
                     :tx              ::sut/tx
                     :ping-events     ::sut/ping-events
                     :payload         ::sut/payload
                     :scheduler-pool  ::sut/scheduler-pool
                     :*udp-server     ::sut/*udp-server})

      (is (uuid? (.id node-object)) "Should be UUID")
      (is (= cluster (.cluster node-object)) "Should be Cluster value")
      (is (s/valid? ::sut/payload (.payload node-object)) "Should be payload value")
      (is (s/valid? ::sut/neighbours (.neighbours node-object)) "Should be neighbours value")
      (is (s/valid? ::sut/status (.status node-object)) "Should be status value")))

  (testing "Setters should set correct values"

    (testing "Correct cluster value should set successfully"
      (let [new-cluster (sut/new-cluster (assoc cluster-data :id (random-uuid) :name "cluster2"))
            node-object (sut/new-node-object node-data1 cluster)]
        (is (= cluster (.cluster node-object)) "Node has current cluster value")
        (.set_cluster node-object new-cluster)
        (is (= new-cluster (.cluster node-object)) "Node has new cluster value")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid cluster data"
                (.set_cluster node-object (assoc cluster :id 1)))))

        (testing "Cluster change allowed only in stopped status"
          (is (thrown-with-msg? Exception #"Node is not stopped"
                (swap! (:*node node-object) assoc :status :leave)
                (.set_cluster node-object new-cluster))))))

    (testing "Correct payload value should set successfully"
      (let [new-payload {:tcp-port 1234 :role "data node"}
            node-object (sut/new-node-object node-data1 cluster)]
        (is (= {} (.payload node-object)) "Node has current (default) payload value")
        (.set_payload node-object new-payload)
        (is (= new-payload (.payload node-object)) "Node has new payload value")))

    (testing "Correct restart counter value should set successfully"
      (let [new-restart-counter 123
            node-object         (sut/new-node-object node-data1 cluster)]
        (is (= 0 (:restart-counter (.value node-object))) "Node has current (default) restart counter value")
        (.set_restart_counter node-object new-restart-counter)
        (is (= new-restart-counter (:restart-counter (.value node-object))) "Node has new restart counter value")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid restart counter data"
                (.set_restart_counter node-object :wrong-value))))))

    (testing "Correct neighbour node value should set successfully"
      (let [neighbour-node (sut/new-neighbour-node neighbour-data1)
            node-object    (sut/new-node-object node-data1 cluster)]
        (is (= {} (.neighbours node-object)) "Node has current (default) neighbours value")
        (.add_neighbour node-object neighbour-node)
        (is (= neighbour-node (get (.neighbours node-object) (:id neighbour-node))) "Node has new neighbours value")

        (testing "Wrong data is caught by spec"
          (is (thrown-with-msg? Exception #"Invalid neighbour node data"
                (.add_neighbour node-object {:a :bad-value}))))))

    (testing "Neighbour node is deleted successfully successfully"
      (let [neighbour-node1 (sut/new-neighbour-node neighbour-data1)
            neighbour-node2 (sut/new-neighbour-node neighbour-data2)
            neighbour-node3 (sut/new-neighbour-node neighbour-data3)
            node-object     (sut/new-node-object node-data1 cluster)]
        (.add_neighbour node-object neighbour-node1)
        (.add_neighbour node-object neighbour-node2)
        (.add_neighbour node-object neighbour-node3)
        (is (= neighbour-node1 (get (.neighbours node-object) (:id neighbour-node1))) "Neighbour1 is present")

        (.delete_neighbour node-object (:id neighbour-node1))
        (is (= (keys (.neighbours node-object)) (map :id [neighbour-node2 neighbour-node3]))
          "Neighbour1 should not present")))))


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



(deftest new-iv-12-test
  (testing "IV for AES/GCM should be 12 bytes length"
    (let [iv12 (sut/new-iv-12)]
      (match (alength iv12) 12))))


(deftest gen-secret-key-test

  (testing "Generating secret key from string token is successful"
    (let [secret-token "qMkaS3^%&%@lOIOJN7h7sbrgojv"
          result       (sut/gen-secret-key secret-token)]
      (is (bytes? result) "Should be bytes array")
      (is (= 32 (alength result)) "Secret key hould be 256-bit length")))

  (testing "Secret keys from the same token are always the same"
    (let [secret-token "qMkaS3^%&%@lOIOJN7h7sqbrgojv"
          secret-bytes1 (sut/gen-secret-key secret-token)
          secret-bytes2 (sut/gen-secret-key secret-token)]
      (match (into [] secret-bytes1) (into [] secret-bytes2))))

  (testing "Secret keys from different tokens are NOT equal"
    (let [password1     "1234567890"
          password2     "123456789"
          secret-bytes1 (sut/gen-secret-key password1)
          secret-bytes2 (sut/gen-secret-key password2)]
      (is (not= (into [] secret-bytes1) (into [] secret-bytes2))))))


(deftest init-cipher-test

  (testing "Init cipher in encryption mode is successful"
    (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :encrypt (.getBytes "1234567890ab"))]
      (is (instance? Cipher cipher) "Should be Cipher instance")))

  (testing "Init cipher in decryption mode is successful"
    (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :decrypt (.getBytes "1234567890ab"))]
      (is (instance? Cipher cipher) "Should be Cipher instance")))

  (testing "Wrong cipher mode is prohibited"
    (is (thrown-with-msg? Exception #"Wrong cipher mode"
          (sut/init-cipher (sut/gen-secret-key "123456") :bad-mode (.getBytes "1234567890ab"))))))



(deftest encrypt-bytes-test

  (testing "Encryption is successful"
    (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :encrypt (.getBytes "1234567890ab"))
          plain-text "Suppose the original message has length = 50 bytes"
          result (sut/encrypt-bytes cipher (.getBytes plain-text))]
      (is (bytes? result) "Should be bytes array")
      (is (> (count result) (.length plain-text)) "Encrypted bytes size should be more than plain text"))))


(deftest decrypt-bytes-test

  (testing "Decryption of encrypted text is successful"
    (let [secret-key (sut/gen-secret-key "123456")
          iv (.getBytes "1234567890ab")
          e-cipher (sut/init-cipher secret-key :encrypt iv)
          d-cipher (sut/init-cipher secret-key :decrypt iv)
          plain-text "Suppose the original message has length = 50 bytes"
          encrypted-bytes (sut/encrypt-bytes e-cipher (.getBytes plain-text))
          corrupted-bytes (byte-array (update (into [] encrypted-bytes) 4 inc))
          result (String. (sut/decrypt-bytes d-cipher encrypted-bytes))]

      (is (= plain-text result) "Original text and decrypted text should be the same")

      (testing "Corrupted data will be not decrypted"
        (is (thrown-with-msg? Exception #"Tag mismatch"
              (sut/decrypt-bytes d-cipher corrupted-bytes)))))))


;;;;;;;;;;


(def node-object1 (sut/new-node-object node-data1 cluster))
(def node-object2 (sut/new-node-object node-data2 cluster))
(def node-object3 (sut/new-node-object node-data3 cluster))


;;
;;(deftest new-node-test
;;  (testing "Create Node instance is successful"
;;    (let [result       (sut/new-node node1-data)
;;          expected-set #{:id :name :host :port :cluster :continue? :status :neighbours-table :*udp-server
;;                         :restart-counter :scheduler-pool :tx-counter :ping-ids :ping-data :tags}]
;;      (is (instance? NodeObject result) "Should be a NodeObject")
;;      (is (instance? Node @(:*node result)) "Atom should contain a Node instance")
;;      (is (= (set (keys @(:*node result))) expected-set) "Key set in a Node instance should be as expected")
;;      (is (thrown-with-msg? Exception #"Node values should correspond to spec"
;;            (sut/new-node {:a 1}))))))
;;
;;
;;(deftest node-start-test
;;  (testing "Node start is successful"
;;    (let [node-object (sut/new-node node1-data)]
;;      (sut/node-start node-object (fn [data] (prn "received: " (String. ^bytes data))))
;;      (is (s/valid? ::sut/*udp-server @(:*udp-server @(:*node node-object))) "Node should have valid UDP server structure")
;;      (is (#{:leave} (:status (sut/node-value node-object))) "Node should have valid status")
;;      (sut/node-stop node-object))))
;;
;;
;;(deftest node-stop-test
;;  (testing "Node stop is successful"
;;    (let [node-object    (sut/new-node node1-data)
;;          scheduler-pool (:scheduler-pool (sut/node-value node-object))]
;;      (sut/node-start node-object (fn [data] (prn "received: " (String. ^bytes data))))
;;      (sut/node-stop node-object)
;;
;;      (let [scheduler-pool-new (:scheduler-pool (sut/node-value node-object))
;;            stopped-udp-server @(:*udp-server (sut/node-value node-object))]
;;
;;        (is (= scheduler-pool scheduler-pool-new)
;;          "Scheduler pool should be the same object")
;;
;;        (is (= @(:pool-atom scheduler-pool) @(:pool-atom scheduler-pool-new))
;;          "Value of scheduler pool contains new pool after reset")
;;
;;        (is (s/valid? ::sut/*udp-server stopped-udp-server)
;;          "Stopped UDP server should have valid structure")
;;
;;        (is (#{:stopped} (:status (sut/node-value node-object))) "Node should have stopped status")))))
;;
;;

;;;;;;;;;;;;
;;
;;(deftest new-ping-test
;;
;;  (testing "PingEvent creation"
;;    (let [node1  (sut/new-node node1-data)
;;          result (sut/new-ping (sut/node-value node1) (random-uuid))]
;;
;;      (is (= PingEvent (type result)) "PingEvent has correct type")
;;
;;      (is (= #{:cmd-type :id :restart-counter :tx-counter :receiver-id}
;;            (into #{} (keys result))) "PingEvent has expected keys")
;;
;;      (testing "PingEvent has correct structure"
;;        (match result {:cmd-type        0
;;                       :id              uuid?
;;                       :restart-counter nat-int?
;;                       :tx-counter      nat-int?
;;                       :receiver-id     uuid?})))))
;;
;;
;;(deftest empty-ping-test
;;
;;  (testing "Empty PingEvent has correct structure"
;;    (let [result (sut/empty-ping)]
;;
;;      (is (= PingEvent (type result)) "PingEvent has correct type")
;;
;;      (match result {:cmd-type        0
;;                     :id              #uuid"00000000-0000-0000-0000-000000000000"
;;                     :restart-counter 0
;;                     :tx-counter      0
;;                     :receiver-id     #uuid"00000000-0000-0000-0000-000000000000"}))))
;;
;;
;;(deftest map->PingEvent-test
;;
;;  (testing "Prepare PingEvent to vector"
;;    (let [node1  (sut/new-node node1-data)
;;          ping1  (sut/new-ping (sut/node-value node1) (random-uuid))
;;          result (.prepare ping1)]
;;
;;      (match result [0 uuid? 0 0 uuid?])
;;      (match result [(:ping sut/event-code) (.-id ping1) (.-restart_counter ping1) (.-tx_counter ping1) (.-receiver_id ping1)])))
;;
;;  (testing "Restore PingEvent from vector"
;;
;;    (let [v      [0 #uuid "742b6766-2867-46b9-b9b1-828f7dbaeb2a" 1 2 #uuid "5be622f2-8600-4c13-8298-4795f7f000c9"]
;;          result (.restore (sut/empty-ping) v)]
;;
;;      (is (= PingEvent (type result)))
;;
;;      (match result {:cmd-type        0
;;                     :id              #uuid "742b6766-2867-46b9-b9b1-828f7dbaeb2a"
;;                     :restart-counter 1
;;                     :tx-counter      2
;;                     :receiver-id     #uuid "5be622f2-8600-4c13-8298-4795f7f000c9"})
;;
;;      (is (thrown-with-msg? Exception #"PingEvent vector has invalid structure"
;;            (.restore (sut/empty-ping) [])))
;;
;;      (testing "Wrong command type code"
;;        (is (thrown-with-msg? Exception #"PingEvent vector has invalid structure"
;;              (.restore (sut/empty-ping) [1 #uuid "742b6766-2867-46b9-b9b1-828f7dbaeb2a" 1 2 #uuid "5be622f2-8600-4c13-8298-4795f7f000c9"]))))
;;
;;      (testing "Wrong structure"
;;        (is (thrown-with-msg? Exception #"PingEvent vector has invalid structure"
;;              (.restore (sut/empty-ping) [0 1 2 3 4])))))))
;;
;;
;;;;;;;;;;;;
;;
;;
;;(deftest new-ack-test
;;  (testing "AckEvent"
;;    (let [node1  (sut/new-node node1-data)
;;          node2  (sut/new-node node2-data)
;;          ping   (sut/new-ping (sut/node-value node1) (-> node2 sut/node-value :id))
;;          result (sut/new-ack (sut/node-value node2) ping)]
;;
;;      (is (= AckEvent (type result)) "PingEvent has correct type")
;;
;;      (is (= #{:cmd-type :id :restart-counter :tx-counter :receiver-id :receiver-tx-counter}
;;            (into #{} (keys result))) "AckEvent has keys as expected")
;;
;;      (testing "AckEvent has correct structure"
;;        (match result {:cmd-type            1
;;                       :id                  (-> node2 sut/node-value :id)
;;                       :restart-counter     nat-int?
;;                       :tx-counter          nat-int?
;;                       :receiver-id         uuid?
;;                       :receiver-tx-counter nat-int?})))))
;;
;;
;;
;;
;;
;;
;;
;;
;;(deftest map->AckEvent-test)










