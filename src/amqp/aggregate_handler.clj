(ns amqp.aggregate-handler
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [clj-time.coerce :as c])
  (:use [repository.entity-manager]
        [repository.aggregate]
        [repository.status]
        [twitter.status]
        [command.generate-timely-statuses]))

(defn build-relationships
  "Build relationships missing between statuses and aggregates"
  [screen-name aggregate-id entity-manager unavailable-aggregate-message]
  ; do not process aggregate with id #1 (taken care of by another command)
  (when (not= 1 aggregate-id)
    (let [{member-model :members
           status-model :status
           status-aggregate-model :status-aggregate
           aggregate-model :aggregate} entity-manager
          aggregate (get-aggregate-by-id aggregate-id aggregate-model unavailable-aggregate-message)
          aggregate-name (:name aggregate)
          member (first (find-member-by-screen-name screen-name member-model))
          found-statuses (find-statuses-for-aggregate-authored-by [(:screen-name member)] aggregate-name)
          {total-new-relationships :total-new-relationships} (new-relationships
                                                               aggregate
                                                               found-statuses
                                                               status-aggregate-model
                                                               status-model)
          total-new-statuses (count found-statuses)
          _ (when (pos? total-new-relationships)
            (log-new-relationships-between-aggregate-and-statuses
              total-new-relationships
              total-new-statuses
              aggregate-name))
          statuses-sorted-by-date (sort-by :created-at found-statuses)
          first-status (first statuses-sorted-by-date)
          since (t/year (c/from-long (:created-at first-status)))
          last-status (last statuses-sorted-by-date)
          until (t/year (c/from-long (:created-at last-status)))]
      (loop [year since]
        (when (<= year until)
          (log/info (str "About to generate timely statuses from " year " for \"" aggregate-name "\""))
          (generate-timely-statuses-for-aggregate aggregate-name year :in-parallel)
          (recur (inc year)))))))
