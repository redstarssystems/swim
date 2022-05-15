(ns org.rssys.swim
  "SWIM functions, specs and domain entities"
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as string]
    [soothe.core :as sth])
  (:import
    (clojure.lang
      Atom)
    (java.time
      LocalDateTime)))


;;;;;;;;;;;
;; SWIM spec
;;;;;;;;;;;


;; not empty string
(s/def ::ne-string #(s/and string? (complement string/blank?)))
(sth/def ::ne-string "Should be not a blank string.")

(s/def ::timestamp #(instance? LocalDateTime %))
(sth/def ::timestamp "Should be LocalDateTime instance.")


;; network specs
(s/def ::host ::ne-string)

(s/def ::port (s/and pos-int? #(< 1023 % 65536)))
(sth/def ::port "Should be a valid port number")


;; common specs
(s/def ::id uuid?)
(s/def ::name ::ne-string)
(s/def ::description string?)
(s/def ::secret-key ::ne-string)
(s/def ::start-time ::timestamp)

(s/def ::server-state #{:running :stopped})
(sth/def ::server-state (format "Should be one of %s" (s/describe ::server-state)))


;; cluster specs
(s/def ::root-node (s/keys :req-un [::host ::port]))
(s/def ::root-nodes (s/coll-of ::root-node))
(s/def ::tag ::ne-string)
(s/def ::tags (s/coll-of ::tag))
(s/def ::nspace ::ne-string)
(s/def ::cluster (s/keys :req-un [::id ::name ::description ::secret-key ::root-nodes ::nspace ::tags]))


;; node specs
(s/def ::status #{:normal :dead :suspicious :leave})
(sth/def ::status (format "Should be one of %s" (s/describe ::status)))

(s/def ::access #{:direct :indirect})
(sth/def ::access (format "Should be one of %s" (s/describe ::access)))

(s/def ::neighbour-descriptor (s/keys :req-un [::id ::status ::access]))
(s/def ::neighbours-table (s/map-of ::id ::neighbour-descriptor))

(s/def ::restart-counter pos-int?)
(s/def ::tx-counter pos-int?)

(s/def ::max-packet-size pos-int?)
(s/def ::continue? boolean?)
(s/def ::server-packet-count pos-int?)
(s/def ::*udp-server (s/keys :req-un [::host ::port ::start-time ::max-packet-size ::server-state ::continue? ::server-packet-count]))
(s/def ::object any?)
(s/def ::scheduler-pool ::object)
(s/def ::ping-ids (s/coll-of ::id))
(s/def ::ping-data (s/map-of ::id ::object))
(s/def ::suspicious-node-ids (s/coll-of ::id))
(s/def ::state (s/keys :req-un [::id
                                ::name
                                ::host
                                ::port
                                ::cluster
                                ::continue?
                                ::status
                                ::neighbours-table
                                ::*udp-server
                                ::restart-counter
                                ::scheduler-pool
                                ::tx-counter
                                ::ping-ids
                                ::ping-data
                                ::tags]))


(s/def ::*state
  ;; this node state

  (s/and
    #(instance? Atom %)
    #(s/valid? ::state (deref %))))



