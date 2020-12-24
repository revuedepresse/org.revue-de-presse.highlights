(ns amqp.status-handler
  (:require [clojure.tools.logging :as log]
            [clj-time.format :as f])
  (:use [repository.entity-manager]
        [repository.aggregate]
        [repository.member]
        [amqp.handling-errors]
        [twitter.api-client]
        [twitter.date]
        [twitter.status]))

(defn get-next-batch-of-statuses-for-member
  [member token-model token-type-model]
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
                      :since-id    (inc (Long/parseLong max-id))}
                     token-model
                     token-type-model)
                   (get-statuses-of-member
                     {:screen-name screen-name
                      :max-id      (if (nil? min-id)
                                     nil
                                     (dec (Long/parseLong min-id)))}
                     token-model
                     token-type-model))
        next-batch-of-statuses (when (and
                                       max-id
                                       (= (count statuses) 0))
                                 (get-statuses-of-member
                                   {:screen-name screen-name
                                    :max-id      (if (nil? min-id)
                                                   nil
                                                   (dec (Long/parseLong min-id)))}
                                   token-model
                                   token-type-model))]
    (if (pos? (count statuses))
      statuses
      next-batch-of-statuses)))

(defn update-status-related-props-for-member
  [member aggregate entity-manager]
  (let [screen-name (:screen-name member)
        member-model (:members entity-manager)
        token-model (:token entity-manager)
        token-type-model (:token-type entity-manager)
        member-id (:id member)
        latest-statuses (get-statuses-of-member {:screen-name screen-name} token-model token-type-model)
        _ (cache-statuses-along-with-authors latest-statuses screen-name aggregate entity-manager)
        last-status (first latest-statuses)
        latest-status-id (:id_str last-status)
        latest-status-publication-date (:created_at last-status)
        mysql-formatted-publication-date (when (some? latest-status-publication-date)
                                           (f/unparse db-date-formatter
                                                      (f/parse date-formatter latest-status-publication-date)))]
    (when (some? mysql-formatted-publication-date)
      (update-status-related-props-for-member-having-id latest-status-id mysql-formatted-publication-date member-id member-model))))

(defn process-lists
  [{screen-name                   :screen-name
    aggregate-id                  :aggregate-id
    entity-manager                :entity-manager
    unavailable-aggregate-message :unavailable-aggregate-message}]
  ; do not process aggregate with id #1 (taken care of by another command)
  (when (not= 1 aggregate-id)
    (let [{member-model     :members
           token-model      :token
           token-type-model :token-type
           aggregate-model  :aggregate} entity-manager
          aggregate (get-aggregate-by-id aggregate-id aggregate-model unavailable-aggregate-message)
          member (first (find-member-by-screen-name screen-name member-model))
          statuses (get-next-batch-of-statuses-for-member member token-model token-type-model)
          processed-relationships (cache-statuses-along-with-authors statuses screen-name aggregate entity-manager)
          last-relationship (last processed-relationships)
          twitter-id-of-status-in-last-relationship (:twitter-id last-relationship)]

      (when
        (and
          (some? last-relationship)
          (some? member))
        (update-min-status-id-for-member-having-id twitter-id-of-status-in-last-relationship
                                                   (:id member)
                                                   member-model))

      (if (pos? (count processed-relationships))
        (process-lists {:screen-name                   screen-name
                        :aggregate-id                  aggregate-id
                        :entity-manager                entity-manager
                        :unavailable-aggregate-message unavailable-aggregate-message})
        (update-status-related-props-for-member member aggregate entity-manager)))))
