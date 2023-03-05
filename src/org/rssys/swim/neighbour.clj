(ns org.rssys.swim.neighbour
  (:require [clojure.spec.alpha :as s]
            [org.rssys.swim.spec :as spec]
            [org.rssys.swim.util :refer [d> safe exec-time serialize]]
            [org.rssys.swim.node :as node])
  (:import (clojure.lang Keyword PersistentHashSet)
           (java.util UUID)
           (org.rssys.swim.node NodeObject)))


(defrecord NeighbourNode [id
                          host
                          port
                          status
                          access
                          restart-counter
                          events-tx
                          payload
                          updated-at])



(defn new-neighbour
  "Create new NeighbourNode instance.

  Required params:
   `id` - uuid value
   `host`  - hostname or ip address.
   `port`  - UDP port.

   Returns new NeighbourNode instance"
  [{:keys [id host port status access restart-counter payload]}]
  (let [neighbour (map->NeighbourNode
                    {:id              id
                     :host            host
                     :port            port
                     :status          (or status :unknown)
                     :access          (or access :direct)
                     :restart-counter (or restart-counter 0)
                     :events-tx       {}
                     :payload         (or payload {})
                     :updated-at      (System/currentTimeMillis)})]
    (if-not (s/valid? ::spec/neighbour-node neighbour)
      (throw (ex-info "Invalid neighbour data" (->> neighbour (s/explain-data ::spec/neighbour-node) spec/problems)))
      neighbour)))


(defn get-neighbour
  "Get NeighbourNode by id if exists or nil if absent."
  ^NeighbourNode
  [^NodeObject this ^UUID id]
  (get (node/get-neighbours this) id))


(defn neighbour-exist?
  "Returns true if neighbour exist, otherwise false"
  ^Boolean
  [^NodeObject this ^UUID id]
  (boolean (get-neighbour this id)))


(defn upsert-neighbour
  "Insert new or update existing neighbour.
  Do nothing if `this` id equals to `neighbour` id.
  Returns void."
  [^NodeObject this ^NeighbourNode neighbour]

  (when-not (s/valid? ::spec/neighbour-node neighbour)
    (throw (ex-info "Invalid neighbour node data"
             (->> neighbour (s/explain-data ::spec/neighbour-node) spec/problems))))

  (when (and (not (neighbour-exist? this (:id neighbour))) (node/cluster-size-exceed? this))
    (d> this :upsert-neighbour-cluster-size-exceeded-error {:nodes-in-cluster (node/nodes-in-cluster this)
                                                            :cluster-size     (node/get-cluster-size this)})
    (throw (ex-info "Cluster size exceeded" {:nodes-in-cluster (node/nodes-in-cluster this)
                                             :cluster-size     (node/get-cluster-size this)})))

  (when-not (= (node/get-id this) (.-id neighbour))
    (d> this :upsert-neighbour {:neighbour-node neighbour})
    (swap! (:*node this) assoc-in [:neighbours (.-id neighbour)]
      (assoc neighbour :updated-at (System/currentTimeMillis)))))


(defn delete-neighbour
  "Delete neighbour from neighbours map"
  [^NodeObject this ^UUID neighbour-id]
  (when (neighbour-exist? this neighbour-id)
    (d> this :delete-neighbour {:neighbour-id neighbour-id})
    (swap! (:*node this) assoc :neighbours (dissoc (node/get-neighbours this) neighbour-id))))


(defn delete-neighbours
  "Delete all neighbours from neighbours map.
  Returns number of deleted neighbours."
  [^NodeObject this]
  (let [nb-count (count (node/get-neighbours this))]
    (d> this :delete-neighbours {:deleted-num nb-count})
    (swap! (:*node this) assoc :neighbours {})
    nb-count))


(defn get-payload
  "Get neighbour payload. If neighbour not exist returns nil.
  Returns map."
  [^NodeObject this ^UUID neighbour-id]
  (when-let [nb (get-neighbour this neighbour-id)]
    (.-payload nb)))


(defn suitable-restart-counter?
  "Check restart counter is suitable from event.
  It should be greater or equal than known restart counter for neighbour.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this event]
  (let [neighbour-id (:id event)]
    (when-let [nb ^NeighbourNode (get-neighbour this neighbour-id)]
      (>= (:restart-counter event) (.-restart_counter nb)))))


(defn suitable-tx?
  "Check tx is suitable from event.
  It should be greater than known tx for neighbour.
  Returns true, if suitable and false/nil if not."
  [^NodeObject this event]
  (let [neighbour-id (:id event)]
    (when-let [nb ^NeighbourNode (get-neighbour this neighbour-id)]
      (> (:tx event) (get (.-events_tx nb) (.event_code event) 0)))))


(defn suitable-incarnation?
  "Check incarnation [restart-counter event-tx] is suitable from event.
  Returns true, if suitable and false if not."
  [^NodeObject this event]
  (= [true true] [(suitable-restart-counter? this event)
                  (suitable-tx? this event)]))


(defn get-neighbours-with-status
  "Returns vector of neighbours with desired statuses.
  Params:
    * `status-set`- set of desired statuses. e.g #{:left :alive}"
  [^NodeObject this ^PersistentHashSet status-set]
  (->> this node/get-neighbours vals (filterv #(status-set (:status %)))))


(defn get-alive-neighbours
  "Returns vector of alive neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:alive}))


(defn get-stopped-neighbours
  "Returns vector of stopped neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:stop}))


(defn get-left-neighbours
  "Returns vector of left neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:left}))


(defn get-dead-neighbours
  "Returns vector of left neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:dead}))


(defn get-suspect-neighbours
  "Returns vector of suspect neighbours."
  [^NodeObject this]
  (get-neighbours-with-status this #{:suspect}))



(defn get-oldest-neighbour
  "Returns the oldest neighbour  with particular status by :updated-at attribute.
  If `status-set` parameter is omitted then use all statuses"
  ([^NodeObject this]
   (get-oldest-neighbour this spec/status-set))
  ([^NodeObject this ^PersistentHashSet status-set]
   (let [desired-nb (get-neighbours-with-status this status-set)]
     (->> desired-nb (sort-by :updated-at) first))))


(defn alive-neighbour?
  "Returns true if neighbour has alive statuses."
  [^NeighbourNode nb]
  (boolean (node/alive-status-set (:status nb))))


(defn set-event-tx
  "Set event :tx for neighbour in neighbours map.
  If new tx value is less or equals to current :tx value then do nothing.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id event]
  (when-let [nb (get-neighbour this neighbour-id)]
    (let [e-key      (:event-code event)
          current-tx (get (.-events_tx nb) e-key 0)
          new-tx     (:tx event)]
      (when (> new-tx current-tx)
        ;;    (upsert-neighbour this (assoc nb :events-tx (assoc events-tx e-key new-tx)))
        (swap! (:*node this) update-in [:neighbours neighbour-id]
          (fn [nb] (assoc nb :events-tx (assoc (.-events_tx nb) e-key new-tx)
                             :updated-at (System/currentTimeMillis))))))))


(defn set-restart-counter
  "Set :restart-counter for neighbour in neighbours map.
  If new restart counter value is less or equals to current :restart-counter value then do nothing.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id ^long new-restart-counter]
  (when-let [nb (get-neighbour this neighbour-id)]
    (when-not (s/valid? ::spec/restart-counter new-restart-counter)
      (throw (ex-info "Invalid restart counter value"
               (->> new-restart-counter (s/explain-data ::spec/restart-counter) spec/problems))))
    (when (> new-restart-counter (.-restart_counter nb))
      (d> this :set-restart-counter {:new-restart-counter new-restart-counter})
      (swap! (:*node this) update-in [:neighbours neighbour-id]
        (fn [nb] (assoc nb :restart-counter new-restart-counter :updated-at (System/currentTimeMillis)))))))


(defn set-status
  "Set :status field for neighbour in neighbours map.
  Status should be one of: #{:stop :join :alive :suspect :left :dead :unknown}
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id ^Keyword status]
  (when (neighbour-exist? this neighbour-id)
    (when-not (s/valid? ::spec/status status)
      (throw (ex-info "Invalid status value" (->> status (s/explain-data ::spec/status) spec/problems))))
    (d> this :set-status {:new-status status})
    (swap! (:*node this) update-in [:neighbours neighbour-id]
      (fn [nb] (assoc nb :status status :updated-at (System/currentTimeMillis))))))


(defn set-dead-status
  [^NodeObject this ^UUID neighbour-id]
  (set-status this neighbour-id :dead))


(defn set-left-status
  [^NodeObject this ^UUID neighbour-id]
  (set-status this neighbour-id :left))


(defn set-alive-status
  [^NodeObject this ^UUID neighbour-id]
  (set-status this neighbour-id :alive))


(defn set-suspect-status
  [^NodeObject this ^UUID neighbour-id]
  (set-status this neighbour-id :suspect))


(defn set-direct-access
  "Set :direct access for neighbour in neighbours map.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id]
  (when (neighbour-exist? this neighbour-id)
    (d> this :set-direct-access {})
    (swap! (:*node this) update-in [:neighbours neighbour-id]
      (fn [nb] (assoc nb :access :direct :updated-at (System/currentTimeMillis))))))


(defn set-indirect-access
  "Set :indirect access for neighbour in neighbours map.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id]
  (when (neighbour-exist? this neighbour-id)
    (d> this :set-indirect-access {})
    (swap! (:*node this) update-in [:neighbours neighbour-id]
      (fn [nb] (assoc nb :access :indirect :updated-at (System/currentTimeMillis))))))



(defn set-payload
  "Set payload for neighbour in neighbours map.
  If neighbour not exist then do nothing.
  Returns void."
  [^NodeObject this ^UUID neighbour-id payload]
  (let [max-payload-size (:max-payload-size (node/get-config this))
        actual-size (alength ^bytes (serialize payload))]
    (when (> actual-size max-payload-size)
      (d> this :set-payload-too-big-error
        {:max-allowed max-payload-size :actual-size actual-size :neighbour-id neighbour-id})
      (throw (ex-info "Payload size for neighbour is too big"
               {:max-allowed max-payload-size :actual-size actual-size :neighbour-id neighbour-id}))))
  (when (neighbour-exist? this neighbour-id)
    (swap! (:*node this) update-in [:neighbours neighbour-id]
      (fn [nb] (assoc nb :payload payload :updated-at (System/currentTimeMillis))))))
