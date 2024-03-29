(ns org.rssys.swim.udp
  "UDP server functions"
  (:require
    [org.rssys.swim.metric :as metric]
    [org.rssys.swim.util :refer [exec-time safe]]
    [org.rssys.swim.vthread :refer [vthread]])
  (:import
    (java.net
      DatagramPacket
      DatagramSocket
      InetAddress
      SocketTimeoutException)
    (java.time
      Instant)))


(defn send-packet
  "Send bytes via UDP to the specified host and port.
  Returns length of sent data in bytes."
  [^bytes buffer ^String host ^long port]
  (let [send-socket (DatagramSocket.)
        length      (alength buffer)
        address     (InetAddress/getByName host)
        packet      (DatagramPacket. buffer length address port)]
    (.send send-socket packet)
    (.close send-socket)
    length))


(defn start-server
  "Starts UDP server in a new Virtual Thread using given host and port.
  Server will process each incoming UDP packets with call-back function in a new Virtual Thread (Java 19+).
  Empty UDP packets are ignored.
  Set `:process-udp-packet-max-ms` gauge metric for maximum detected execution time of UDP packet processing.
  Set `:packet-per-sec` gauge metric with mean number of incoming packets per second since start.
  Returns an atom with a map with the following structure:
    {:node-id              uuid
     :host                 `host`
     :port                 `port`
     :start-time           (Instant/now)
     :max-packet-size      `max-packet-size`
     :server-state         :running
     :continue?            true
     :server-packet-count 0 }.

  Params:
  * `host` - ^String hostname or IP address.
  * `port` - ^long port for listening.
  * `process-cb-fn` - call-back function to process data from UDP packets.

  Opts:
  * `timeout` - time in ms which server waits for incoming packet, default is 0 (infinity).
  * `max-packet-size` - max UDP packet size we are ready to accept, default is 1432.
  * `*server-ready-promise` - if promise is present then deliver *server when server is ready to accept UDP."
  [node-id host port process-cb-fn & {:keys [^long timeout ^long max-packet-size *server-ready-promise]
                                      :or   {timeout 0 max-packet-size 1432}}]
  (try
    (let [*server       (atom
                          {:node-id         node-id
                           :host            host
                           :port            port
                           :start-time      (Instant/now)
                           :max-packet-size max-packet-size
                           :server-state    :running
                           :continue?       true
                           :packet-count    0})
          server-socket (DatagramSocket. port (InetAddress/getByName host))]
      (.setSoTimeout server-socket timeout)
      (metric/gauge metric/registry :process-udp-packet-max-ms {:node-id node-id} 0)
      (vthread
        (when *server-ready-promise (deliver *server-ready-promise *server))

        (while (-> @*server :continue?)
          (let [buffer ^bytes (make-array Byte/TYPE max-packet-size)
                packet (DatagramPacket. buffer (alength buffer))]
            (try
              (.receive server-socket packet)
              (vthread
                (let [start-time (- (.getEpochSecond (Instant/now)) (.getEpochSecond (:start-time @*server)))
                      packets-since-start (double (:packet-count @*server))
                      packet-per-sec (or (safe (int (Math/floor (/ packets-since-start start-time)))) 0)]
                  (swap! *server update :packet-count inc)
                  (safe (metric/gauge metric/registry :packet-per-sec {:node-id node-id} packet-per-sec))
                  (when (pos? (.getLength packet))
                    (let [data (byte-array (.getLength packet) (.getData packet))]
                      (exec-time node-id :process-udp-packet-max-ms (process-cb-fn data))))))
              (catch SocketTimeoutException _)
              (catch Exception e
                (.close server-socket)
                (throw e)))))

        (.close server-socket)
        (swap! *server assoc :server-state :stopped))
      *server)
    (catch Exception e
      (throw (ex-info "Can't start node" {:host host :port port} e)))))


(defn server-value
  [*server]
  (-> @*server :server-state))


(defn stop-server
  "Stop UDP server which run by `start-server`.
  Returns: *server."
  [*server]
  (let [*stop-complete (promise)
        {:keys [host port]} @*server]

    (add-watch *server :stop-watcher
      (fn [_ a _ new-state]
        (when (or
                (= :stopped (-> @a :server-state))
                (= :stopped (:server-state new-state)))
          (deliver *stop-complete :stopped)
          @*stop-complete)))

    (swap! *server assoc :continue? false)

    (send-packet (.getBytes "") host port)                 ;; send empty packet to trigger server

    (deref *stop-complete 300 :timeout)
    (when-not (= :stopped @*stop-complete)
      (throw (ex-info "Can't stop server" @*server)))      ;; wait for packet reach the server
    (remove-watch *server :stop-watcher)
    *server))


(defn packets-received
  [*server-map]
  (-> @*server-map :packet-count))
