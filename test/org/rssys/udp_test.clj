(ns org.rssys.udp-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [matcho.core :refer [match]]
    [org.rssys.udp :as sut])
  (:import
    (java.net
      DatagramPacket
      DatagramSocket
      InetAddress
      SocketTimeoutException)
    (java.time
      Instant)))


(defn test-udp-server
  "Creates and run UDP server.
  It reads only one packet and then closes socket.
  Waits until packet arrives or timeout expired if set one.
  If data-promise is present then deliver data to a promise.
  If server-ready-promise is present then deliver event that server is ready to accept UDP.
  Returns received data as a bytes array."
  [{:keys [^String host ^long port ^long timeout *data-promise *server-ready-promise]
    :or   {host "localhost" timeout 0}}]
  (let [server-socket (DatagramSocket. port (InetAddress/getByName host))]
    (try
      (let [buffer (make-array Byte/TYPE 1024)
            packet (DatagramPacket. buffer (alength buffer))]
        (.setSoTimeout server-socket timeout)
        (when *server-ready-promise (future (deliver *server-ready-promise :ready)))
        (.receive server-socket packet)
        (let [data (byte-array (.getLength packet) (.getData packet))]
          (when *data-promise
            (deliver *data-promise data))
          data))
      (catch SocketTimeoutException _
        (when *data-promise (deliver *data-promise :timeout))
        :timeout)
      (finally
        (.close server-socket)))))


(deftest send-packet-test
  (testing "UDP packet send success"
    (let [host                  "localhost"
          port                  (+ 10000 (rand-int 50000))
          *data-promise         (promise)
          *server-ready-promise (promise)
          message               "Hello, world!"]
      (future (test-udp-server {:host                  host
                                :port                  port
                                :timeout               100
                                :*data-promise         *data-promise
                                :*server-ready-promise *server-ready-promise}))
      @*server-ready-promise                                ;; wait until server is ready
      (is (= (.length message) (sut/send-packet (.getBytes message) host port)))
      (is (= (String. ^bytes (deref *data-promise)) message)))))



(deftest start-test
  (let [host                  "localhost"
        port                  (+ 10000 (rand-int 50000))
        *server-ready-promise (promise)
        messages              #{"one" "two" "three" "four" "abcdefg" "12345"}
        *results              (atom #{})
        process-message-fn    (fn [data] (swap! *results conj (String. ^bytes data)))
        *server               (sut/start host port process-message-fn {:*server-ready-promise *server-ready-promise})]
    @*server-ready-promise                                  ;; wait until server is ready
    (doseq [m messages]
      (sut/send-packet (.getBytes ^String m) host port)
      (Thread/sleep 1))
    (let [*stop-result (sut/stop *server)]
      (is (= messages @*results) "All sent messages are equal to received messages")
      (is (>= (sut/packets-received *server) (count messages)) "Number of received packets should be more or equal to sent messages")
      (testing "Server map structure is returned as expected."
        (match @*server {:host                string?
                         :port                number?
                         :start-time          #(instance? Instant %)
                         :max-packet-size     number?
                         :server-state        :stopped
                         :continue?           boolean?
                         :server-packet-count pos?}))
      (is (= (sut/server-value *stop-result) :stopped) "Server is stopped successfully."))))
