(ns org.rssys.swim.udp
  "UDP server functions"
  (:require
    [org.rssys.swim.metric :as metric]
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
  "Send data via UDP to the specified host and port.
  Returns length of sent data in bytes."
  [^bytes data ^String host ^long port]
  (let [send-socket (DatagramSocket.)
        length      (alength data)
        address     (InetAddress/getByName host)
        packet      (DatagramPacket. data length address port)]
    (.send send-socket packet)
    (.close send-socket)
    length))


(defn start
  "Starts UDP server in a new Virtual Thread using given host and port.
  Server will process each incoming UDP packets with call-back function in a new Virtual Thread (Java 19+).
  Empty UDP packets are ignored.
  Returns an atom with a server map with running server parameters:
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
  * `timeout` - time in ms which server waits for incoming packet, default is 0.
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
        (do
          (when *server-ready-promise (deliver *server-ready-promise *server))
          (while (-> @*server :continue?)
            (let [buffer ^bytes (make-array Byte/TYPE max-packet-size)
                  packet (DatagramPacket. buffer (alength buffer))]
              (try
                (.receive server-socket packet)
                (swap! *server update :packet-count inc)
                (if (pos? (.getLength packet))
                  (vthread
                    (let [start-ts (System/currentTimeMillis)
                          _ (process-cb-fn (byte-array (.getLength packet) (.getData packet)))
                          end-ts   (System/currentTimeMillis)
                          diff-max-ts (metric/get-metric metric/registry :process-udp-packet-max-ms {:node-id node-id})
                          diff-ts (- end-ts start-ts)]
                      (when (and diff-max-ts (> diff-ts diff-max-ts))
                        (metric/gauge metric/registry :process-udp-packet-max-ms {:node-id node-id} diff-ts))))
                  :nothing)                                 ;; do not process empty packets
                (catch SocketTimeoutException _)
                (catch Exception e
                  (.close server-socket)
                  (throw e)))))
          (.close server-socket)
          (swap! *server assoc :server-state :stopped)))
      *server)
    (catch Exception e
      (throw (ex-info "Can't start node" {:host host :port port} e)))))


(defn server-value
  [*server]
  (-> @*server :server-state))


(defn stop
  "Stops given `*server`.
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
