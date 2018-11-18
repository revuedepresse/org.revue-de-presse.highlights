(ns amqp.favorite-status-handler
  (:require [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [twitter.api-client]
        [twitter.favorited-status]))

(def error-mismatching-favorites-cols-length "The favorited statuses could not be saved because of missing data.")
(def error-unavailable-aggregate "The aggregate does not seem to be available.")

(defn get-next-batch-of-favorites-for-member
  [member token-model]
  (let [screen-name (:screen-name member)
        max-favorite-id (:max-favorite-status-id member)
        min-favorite-id (:min-favorite-status-id member)
        _ (if max-favorite-id
            (log/info (str "About to fetch favorites since status #" max-favorite-id))
            (log/info (str "About to fetch favorites until reaching status #" min-favorite-id)))
        favorites (if max-favorite-id
                    (get-favorites-of-member
                      {:screen-name screen-name
                       :since-id (inc (Long/parseLong (:max-favorite-status-id member)))}
                      token-model)
                    (get-favorites-of-member
                      {:screen-name screen-name
                       :max-id (if (nil? (:min-favorite-status-id member))
                                 nil
                                 (dec (Long/parseLong (:min-favorite-status-id member))))}
                      token-model))
        next-batch-of-favorites (when (and
                                        max-favorite-id
                                        (= (count favorites) 0))
                                  (get-favorites-of-member
                                    {:screen-name screen-name
                                     :max-id (if (nil? (:min-favorite-status-id member))
                                               nil
                                               (dec (Long/parseLong (:min-favorite-status-id member))))}
                                    token-model))]
    (if (pos? (count favorites))
      favorites
      next-batch-of-favorites)))

(defn update-max-favorite-id-for-member
  [member aggregate entity-manager]
  (let [screen-name (:screen-name member)
        member-model (:members entity-manager)
        token-model (:tokens entity-manager)
        member-id (:id member)
        latest-favorites (get-favorites-of-member {:screen-name screen-name} token-model)
        _ (process-favorites member latest-favorites aggregate entity-manager error-mismatching-favorites-cols-length)
        latest-status-id (:id_str (first latest-favorites))]
    (update-max-favorite-id-for-member-having-id latest-status-id member-id member-model)))

(defn process-likes
  [screen-name aggregate-id entity-manager]
  (let [{member-model :members
         token-model :tokens
         aggregate-model :aggregates} entity-manager
        aggregate (first (find-aggregate-by-id aggregate-id aggregate-model))
        _ (when (nil? (:name aggregate))
            (throw (Exception. (str error-unavailable-aggregate))))
        member (first (find-member-by-screen-name screen-name member-model))
        favorites (get-next-batch-of-favorites-for-member member token-model)
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
      (process-likes screen-name aggregate-id entity-manager)
      (update-max-favorite-id-for-member member aggregate entity-manager))))
