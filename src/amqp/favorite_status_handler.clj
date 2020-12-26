(ns amqp.favorite-status-handler
  (:require [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [repository.publishers-list]
        [repository.member]
        [amqp.handling-errors]
        [twitter.api-client]
        [twitter.favorited-status]))

(defn get-next-batch-of-favorites-for-member
  [member token-model token-type-model]
  (let [screen-name (:screen-name member)
        max-favorite-id (:max-favorite-status-id member)
        min-favorite-id (:min-favorite-status-id member)
        _ (if max-favorite-id
            (log/info (str "About to fetch favorites since status #" max-favorite-id))
            (log/info (str "About to fetch favorites until reaching status #" min-favorite-id)))
        favorites (if max-favorite-id
                    (get-favorites-of-member
                      {:screen-name screen-name
                       :since-id (inc (Long/parseLong max-favorite-id))}
                      token-model token-type-model)
                    (get-favorites-of-member
                      {:screen-name screen-name
                       :max-id (if (nil? min-favorite-id)
                                 nil
                                 (dec (Long/parseLong min-favorite-id)))}
                      token-model token-type-model))
        next-batch-of-favorites (when (and
                                        max-favorite-id
                                        (= (count favorites) 0))
                                  (get-favorites-of-member
                                    {:screen-name screen-name
                                     :max-id (if (nil? min-favorite-id)
                                               nil
                                               (dec (Long/parseLong min-favorite-id)))}
                                    token-model token-type-model))]
    (if (pos? (count favorites))
      favorites
      next-batch-of-favorites)))

(defn update-max-favorite-id-for-member
  [member aggregate entity-manager]
  (let [screen-name (:screen-name member)
        member-model (:members entity-manager)
        token-model (:token entity-manager)
        token-type-model (:token-type entity-manager)
        member-id (:id member)
        latest-favorites (get-favorites-of-member {:screen-name screen-name} token-model token-type-model)
        _ (process-favorites member latest-favorites aggregate entity-manager error-mismatching-favorites-cols-length)
        latest-status-id (:id_str (first latest-favorites))]
    (update-max-favorite-id-for-member-having-id latest-status-id member-id member-model)))

(defn process-likes
  [screen-name aggregate-id entity-manager unavailable-aggregate-message]
  (let [{member-model :members
         token-model :token
         token-type-model :token-type
         aggregate-model :aggregate} entity-manager
        aggregate (get-aggregate-by-id aggregate-id aggregate-model unavailable-aggregate-message)
        member (first (find-member-by-screen-name screen-name member-model))
        favorites (get-next-batch-of-favorites-for-member member token-model token-type-model)
        processed-likes (process-favorites member favorites aggregate entity-manager error-mismatching-favorites-cols-length)
        last-favorited-status (last processed-likes)
        status (:status last-favorited-status)
        favorite-author (:favorite-author last-favorited-status)]

    (when
      (and
        (not (nil? status))
        (not (nil? favorite-author)))
      (update-min-favorite-id-for-member-having-id (:twitter-id status)
                                                   (:id favorite-author)
                                                   member-model))
    (if (pos? (count processed-likes))
      (process-likes screen-name aggregate-id entity-manager unavailable-aggregate-message)
      (update-max-favorite-id-for-member member aggregate entity-manager))))
