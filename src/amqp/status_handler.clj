(ns amqp.status-handler
  (:require [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [repository.aggregate]
        [amqp.handling-errors]
        [twitter.api-client]
        [twitter.status]))

(defn get-next-batch-of-statuses-for-member
  [member token-model]
  (let [screen-name (:screen-name member)
        max-id (:max-status-id member)
        min-id (:min-status-id member)
        min-id (if (= min-id "-INF") nil min-id)
        _ (if max-id
            (log/info (str "About to fetch statuses since status #" max-id))
            (log/info (str "About to fetch statuses until reaching status #" min-id)))
        statuses (if max-id
                    (get-statuses-of-member
                      {:screen-name screen-name
                       :since-id (inc (Long/parseLong max-id))}
                      token-model)
                    (get-statuses-of-member
                      {:screen-name screen-name
                       :max-id (if (nil? min-id)
                                 nil
                                 (dec (Long/parseLong min-id)))}
                      token-model))
        next-batch-of-statuses (when (and
                                        max-id
                                        (= (count statuses) 0))
                                  (get-statuses-of-member
                                    {:screen-name screen-name
                                     :max-id (if (nil? min-id)
                                               nil
                                               (dec (Long/parseLong min-id)))}
                                    token-model))]
    (if (pos? (count statuses))
      statuses
      next-batch-of-statuses)))

(defn update-max-status-id-for-member
  [member aggregate entity-manager]
  (let [screen-name (:screen-name member)
        member-model (:members entity-manager)
        token-model (:tokens entity-manager)
        member-id (:id member)
        latest-statuses (get-statuses-of-member {:screen-name screen-name} token-model)
        _ (cache-statuses-along-with-authors latest-statuses screen-name aggregate entity-manager)
        latest-status-id (:id_str (first latest-statuses))]
    (update-max-status-id-for-member-having-id latest-status-id member-id member-model)))

(defn process-lists
  [screen-name aggregate-id entity-manager unavailable-aggregate-message]
  (let [{member-model :members
         token-model :tokens
         aggregate-model :aggregates} entity-manager
        aggregate (get-aggregate-by-id aggregate-id aggregate-model unavailable-aggregate-message)
        member (first (find-member-by-screen-name screen-name member-model))
        statuses (get-next-batch-of-statuses-for-member member token-model)
        processed-relationships (cache-statuses-along-with-authors statuses screen-name aggregate entity-manager)
        last-relationship (last processed-relationships)
        twitter-id-of-status-in-last-relationship (:twitter-id last-relationship)]

    (when
      (and
        (not (nil? last-relationship))
        (not (nil? member)))
      (update-min-status-id-for-member-having-id twitter-id-of-status-in-last-relationship
                                                 (:id member)
                                                 member-model))
    (if (pos? (count processed-relationships))
      (process-lists screen-name aggregate-id entity-manager unavailable-aggregate-message)
      (update-max-status-id-for-member member aggregate entity-manager))))
