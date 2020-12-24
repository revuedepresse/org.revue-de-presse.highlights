(ns twitter.favorited-status
  (:require [clojure.tools.logging :as log]
            [clj-uuid :as uuid])
  (:use [repository.entity-manager]
        [repository.status]
        [twitter.date]
        [twitter.status]))

(defn get-status-ids
  [status]
  (let [{twitter-id :id_str} status]
    twitter-id))

(defn get-favorites
  [favorites model]
  (let [statuses-ids (pmap get-status-ids favorites)]
    (find-statuses-having-twitter-ids statuses-ids model)))

(defn get-favorite-status-ids
  [total-items]
  (let [id {:id (uuid/v1)}]
    (take total-items (iterate (constantly id) id))))

(defn get-aggregate-properties
  [aggregate total-items]
  (let [aggregate-properties {:aggregate-id   (:id aggregate)
                              :aggregate-name (:name aggregate)}]
    (take total-items (iterate (constantly aggregate-properties) aggregate-properties))))

(defn get-archive-properties
  [total-items]
  (let [archive-properties {:is-archived-status 0}]
    (take total-items (iterate (constantly archive-properties) archive-properties))))

(defn get-favorite-author-properties
  [favorite-author total-items]
  (let [favorite-author-properties {:liked-by             (:id favorite-author)
                                    :liked-by-member-name (:screen-name favorite-author)}]
    (take total-items (iterate (constantly favorite-author-properties) favorite-author-properties))))

(defn get-favorited-status-author-properties
  [favorited-status-author]
  (let [favorited-status-author-properties {:member-id   (:id favorited-status-author)
                                            :member-name (:screen-name favorited-status-author)}]
    favorited-status-author-properties))

(defn get-status-id
  [status]
  (let [status-id {:status-id (:id status)}]
    status-id))

(defn assoc-properties-of-favorited-statuses
  [favorites total-favorites aggregate favorite-author member-model status-model error-message]
  (let [favorite-status-authors (get-statuses-authors favorites member-model)
        favorited-statuses (get-favorites favorites status-model)
        publication-date-cols (get-publication-dates-of-statuses favorites)
        id-col (get-favorite-status-ids total-favorites)
        aggregate-cols (get-aggregate-properties aggregate total-favorites)
        favorited-status-author-cols (pmap get-favorited-status-author-properties favorite-status-authors)
        favorited-status-col (pmap get-status-id favorited-statuses)
        archive-col (get-archive-properties total-favorites)
        favorite-author-cols (get-favorite-author-properties favorite-author total-favorites)
        total-favorite-ids (count id-col)
        total-aggregate-properties (count aggregate-cols)
        total-archive-properties (count archive-col)
        total-favorite-status-authors (count favorite-status-authors)
        total-publication-dates (count publication-date-cols)
        total-favorite-author-properties (count favorite-author-cols)
        total-favorited-status-author-properties (count favorited-status-author-cols)
        total-status-ids (count favorited-status-col)
        total-favorited-statuses (count favorited-statuses)]
    (log/info (str "There are " total-favorite-ids " favorite ids."))
    (log/info (str "There are " total-favorited-statuses " favorited statuses."))
    (log/info (str "There are " total-favorite-status-authors " authors of favorited statuses."))
    (log/info (str "There are " total-publication-dates " publication dates."))
    (log/info (str "There are " total-archive-properties " archive properties."))
    (log/info (str "There are " total-favorite-author-properties " favorite authors properties."))
    (log/info (str "There are " total-favorited-status-author-properties " favorited status authors properties."))
    (log/info (str "There are " total-status-ids " ids of statuses."))
    (log/info (str "There are " total-aggregate-properties " aggregate maps."))
    (if (=
          total-favorite-ids
          total-favorite-status-authors
          total-favorited-statuses
          total-publication-dates
          total-archive-properties
          total-favorite-author-properties
          total-favorited-status-author-properties
          total-status-ids
          total-aggregate-properties)
      {:columns {:id-col                       id-col
                 :publication-date-cols        publication-date-cols
                 :aggregate-cols               aggregate-cols
                 :archive-col                  archive-col
                 :favorited-status-col         favorited-status-col
                 :favorite-author-cols         favorite-author-cols
                 :favorited-status-author-cols favorited-status-author-cols}}
      (throw (Exception. (str error-message))))))

(defn assoc-favorited-status-cols
  [id-col
   publication-date-cols
   aggregate-cols
   archive-cols
   favorited-status-col
   favorited-status-author-cols
   favorite-author-cols]
  (let [parsed-publication-date (:parsed-publication-date publication-date-cols)
        publication-date-time (:mysql-formatted-publication-date publication-date-cols)]
    {:id                    (:id id-col)
     :time-range            (get-time-range parsed-publication-date)
     :publication-date-time publication-date-time
     :is-archived-status    (:is-archived-status archive-cols)
     :aggregate-id          (:aggregate-id aggregate-cols)
     :aggregate-name        (:aggregate-name aggregate-cols)
     :status-id             (:status-id favorited-status-col)
     :liked-by              (:liked-by favorite-author-cols)
     :liked-by-member-name  (:liked-by-member-name favorite-author-cols)
     :member-id             (:member-id favorited-status-author-cols)
     :member-name           (:member-name favorited-status-author-cols)}))

(defn assoc-properties-of-non-empty-favorited-statuses
  [aggregate favorite-author favorites member-model status-model error-message]
  (let [total-favorites (count favorites)]
    (if (pos? total-favorites)
      (do
        (let [{{id-col                       :id-col
                publication-date-cols        :publication-date-cols
                aggregate-cols               :aggregate-cols
                archive-col                  :archive-col
                favorited-status-col         :favorited-status-col
                favorite-author-cols         :favorite-author-cols
                favorited-status-author-cols :favorited-status-author-cols}
               :columns} (assoc-properties-of-favorited-statuses favorites
                                                                 total-favorites
                                                                 aggregate
                                                                 favorite-author
                                                                 member-model
                                                                 status-model
                                                                 error-message)
              liked-statuses-values (doall (map assoc-favorited-status-cols
                                                id-col
                                                publication-date-cols
                                                aggregate-cols
                                                archive-col
                                                favorited-status-col
                                                favorited-status-author-cols
                                                favorite-author-cols))]
          (log/info (str (count liked-statuses-values) " favorited statuses have been accumulated."))
          liked-statuses-values))
      '())))

(defn process-favorites
  [member favorites aggregate {liked-status-model :liked-status
                               member-model       :members
                               status-model       :status
                               token-model        :token
                               token-type-model   :token-type} error-message]
  (let [_ (preprocess-statuses favorites status-model member-model token-model token-type-model)
        favorited-statuses-values (assoc-properties-of-non-empty-favorited-statuses
                                    aggregate
                                    member
                                    favorites
                                    member-model
                                    status-model
                                    error-message)
        new-favorites (new-liked-statuses favorited-statuses-values liked-status-model status-model)]
    (log/info (str "There are " (count new-favorites) " new favorites"))))
