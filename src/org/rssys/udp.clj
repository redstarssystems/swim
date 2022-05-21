(ns org.rssys.udp
  "UDP server functions"
  (:require
    [org.rssys.common :refer [vfuture]])
  (:import
    (java.net
      DatagramPacket
      DatagramSocket
      InetAddress
      SocketTimeoutException)
    (java.time
      LocalDateTime)))


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


(defn server-start
  "Starts UDP server in a new Virtual Thread using given host and port .
  Server will process each incoming UDP packets with provided call-back function in a new Virtual Thread (Java 19+).
  Empty UDP packets are ignored.
  Returns an atom with a server map with running server parameters:
    {:host                 `host`
     :port                 `port`
     :start-time           (LocalDateTime/now)
     :max-packet-size      `max-packet-size`
     :server-state         :running
     :continue?            true
     :server-packet-count 0 }.

  Params:
  * `host` - ^String hostname or IP address.
  * `port` - ^long port for listening.
  * `process-cb-fn` - call-back function to process data from UDP packets.

  Opts:
  * `timeout` - time in ms which server waits for incoming packet, default 0 (infinite).
  * `max-packet-size` - max UDP packet size we are ready to accept, default is 1024.
  * `*server-ready-promise` - if promise is present then deliver *server-map when server is ready to accept UDP."
  [host port process-cb-fn & {:keys [^long timeout ^long max-packet-size *server-ready-promise]
                              :or   {timeout 0 max-packet-size 1024}}]
  (let [*server-map   (atom
                        {:host                host
                         :port                port
                         :start-time          (LocalDateTime/now)
                         :max-packet-size     max-packet-size
                         :server-state        :running
                         :continue?           true
                         :server-packet-count 0})
        server-socket (DatagramSocket. port (InetAddress/getByName host))]
    (.setSoTimeout server-socket timeout)
    (vfuture
      (when *server-ready-promise (deliver *server-ready-promise *server-map))
      (while (-> @*server-map :continue?)
        (let [buffer (make-array Byte/TYPE max-packet-size)
              packet (DatagramPacket. buffer (alength buffer))]
          (try
            (.receive server-socket packet)
            (swap! *server-map update :server-packet-count inc)
            (if (pos? (.getLength packet))
              (vfuture (process-cb-fn (byte-array (.getLength packet) (.getData packet))))
              :nothing)                                      ;; do not process empty packets
            (catch SocketTimeoutException _)
            (catch Exception e
              (.close server-socket)
              (throw e)))))
      (.close server-socket)
      (swap! *server-map assoc :server-state :stopped))
    *server-map))


(defn server-state
  [*server-map]
  (-> @*server-map :server-state))


(defn server-stop
  "Stop server using given `*server-map`.
  Returns: *server-map."
  [*server-map]
  (let [{:keys [host port]} @*server-map]
    (swap! *server-map assoc :continue? false)
    (send-packet (.getBytes "") host port)                  ;; send empty packet to trigger server
    *server-map))


(defn server-packets-received
  [*server-map]
  (-> @*server-map :server-packet-count))




