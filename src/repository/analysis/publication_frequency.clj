(ns repository.analysis.publication-frequency
  (:require [korma.core :as db]
            [clj-uuid :as uuid]
            [utils.error-handler :as error-handler])
  (:use [korma.db]
        [repository.database-schema]
        [utils.string]))

(declare publication-frequency)

(defn get-publication-frequency-model
  [connection]
  (db/defentity publication-frequency
                (db/pk :id)
                (db/table :publication_frequency)
                (db/database connection)
                (db/entity-fields
                  :member_id
                  :sample_id
                  :per_hour_of_day_percentage
                  :per_day_of_week_percentage
                  :per_hour_of_day
                  :per_day_of_week
                  :updated_at))
  publication-frequency)

(defn select-publication-frequencies
  [model member-model sample-model]
  (let [screen-name-col (get-column "usr_twitter_username" member-model)
        total-subscribees-col (get-column "total_subscribees" member-model)
        total-subscriptions-col (get-column "total_subscriptions" member-model)
        max-status-id-col (get-column "max_status_id" member-model)
        min-status-id-col (get-column "min_status_id" member-model)
        not-found-col (get-column "not_found" member-model)
        protected-col (get-column "protected" member-model)
        suspended-col (get-column "suspended" member-model)
        twitter-id-col (get-column "usr_twitter_id" member-model)
        member-id-col (get-column "usr_id" member-model)
        sample-label-col (get-column "label" sample-model)
        sample-id-col (get-column "id" sample-model)]
    (->
      (db/select* model)
      (db/fields [:id]
                 [sample-label-col :sample-label]
                 [sample-id-col :sample-id]
                 [screen-name-col :screen-name]
                 [twitter-id-col :twitter-id]
                 [member-id-col :member-id]
                 [total-subscribees-col :total-subscribees]
                 [total-subscriptions-col :total-subscriptions]
                 [max-status-id-col :max-status-id]
                 [min-status-id-col :min-status-id]
                 [not-found-col :not-found]
                 [protected-col :protected]
                 [suspended-col :suspended]
                 [:per_hour_of_day :per-hour-of-day]
                 [:per_day_of_week :per-day-of-week]
                 [:per_hour_of_day_percentage :per-hour-of-day-percentage]
                 [:per_day_of_week_percentage :per-day-of-week-percentage]
                 [:updated_at :updated-at])
      (db/join member-model (= member-id-col :member_id))
      (db/join sample-model (= sample-id-col :sample_id)))))

(defn find-publication-frequencies-having-column-matching-values
  "Find frequencies of publication per day of week and hour or day which values of a given column
  can be found in collection passed as argument"
  ([column values model member-model sample-model]
   (let [values (if values values '(0))
         matching-records (-> (select-publication-frequencies model member-model sample-model)
                              (db/where {column [in values]})
                              (db/select))]
     (if matching-records
       matching-records
       '()))))

(defn find-publication-frequencies-by-label
  ([label model member-model sample-model]
   (let [sample-label-col (get-column "label" sample-model)]
     (try
       (find-publication-frequencies-having-column-matching-values
         sample-label-col
         [label]
         model
         member-model
         sample-model)
       (catch Exception e
         (error-handler/log-error e))))))

(defn find-publication-frequencies-by-ids
  [ids model member-model sample-model]
  (find-publication-frequencies-having-column-matching-values
    :id
    ids
    model
    member-model
    sample-model))

(defn bulk-insert-from-props
  [props model member-model sample-model]
  (let [identified-props (pmap
                           #(assoc % :id (uuid/to-string
                                           (-> (uuid/v1) (uuid/v5 (:member-id %)))))
                           props)
        snake-cased-props (map snake-case-keys identified-props)
        publication-frequencies-ids (pmap #(:id %) snake-cased-props)]
    (if publication-frequencies-ids
      (do
        (try
          (db/insert model (db/values snake-cased-props))
          (catch Exception e
            (error-handler/log-error e)))
        (find-publication-frequencies-by-ids publication-frequencies-ids model member-model sample-model))
      '())))