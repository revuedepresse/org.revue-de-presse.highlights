(ns twitter.status-hash
  (:require [clojure.tools.logging :as log]
            [pandect.algo.sha1 :refer :all]))

(def ^:dynamic *status-hash-enabled-logging* false)

(defn get-status-hash
  [status]
  (let [twitter-id (:status_id status)
        concatenated-string (str (:text status) twitter-id)
        hash (sha1 concatenated-string)]
    (when *status-hash-enabled-logging*
      (log/info (str "Hash for " twitter-id " is \"" hash "\"")))
    hash))

(defn assoc-hash
  [status]
  (assoc status :hash (get-status-hash status)))
