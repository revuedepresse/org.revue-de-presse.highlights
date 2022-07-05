(ns repository.analysis.sample
  (:require [korma.core :as db]
            [clj-time.local :as l]
            [clj-time.format :as f]
            [clj-uuid :as uuid]
            [utils.error-handler :as error-handler])
  (:use [korma.db]
        [repository.database-schema]
        [utils.string]
        [twitter.date]))

(declare sample)

(defn get-sample-model
  [connection]
  (db/defentity sample
                (db/pk :id)
                (db/table :sample)
                (db/database connection)
                (db/entity-fields
                  :label
                  :created_at))
  sample)

(defn select-samples
  [model member-model publication-frequency-model]
  (let [per-day-of-week-col (get-column "per_day_of_week" publication-frequency-model)
        per-hour-of-day-col (get-column "per_hour_of_day" publication-frequency-model)
        per-day-of-week-percentage-col (get-column "per_day_of_week_percentage" publication-frequency-model)
        per-hour-of-day-percentage-col (get-column "per_hour_of_day_percentage" publication-frequency-model)
        member-col (get-column "member_id" publication-frequency-model)
        sample-id-col (get-column "sample_id" publication-frequency-model)
        screen-name-col (get-column "usr_twitter_username" member-model)
        total-subscribees-col (get-column "total_subscribees" member-model)
        total-subscriptions-col (get-column "total_subscriptions" member-model)
        max-status-id-col (get-column "max_status_id" member-model)
        min-status-id-col (get-column "min_status_id" member-model)
        not-found-col (get-column "not_found" member-model)
        protected-col (get-column "protected" member-model)
        suspended-col (get-column "suspended" member-model)
        twitter-id-col (get-column "usr_twitter_id" member-model)
        member-id-col (get-column "usr_id" member-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [per-hour-of-day-col :per-hour-of-day-col]
                 [per-day-of-week-col :per-day-of-week-col]
                 [per-day-of-week-percentage-col :per-day-of-week-percentage]
                 [per-hour-of-day-percentage-col :per-hour-of-day-percentage]
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
                 [:label]
                 [:created_at :created-at])
      (db/join publication-frequency-model (= sample-id-col :id))
      (db/join member-model (= member-col member-id-col)))))

(defn find-sample-by
  ([column values model member-model publication-frequency-model]
   (let [values (if values values '(0))
         matching-records (-> (select-samples model member-model publication-frequency-model)
                              (db/where {column [in values]})
                              (db/select))]
     (if matching-records
       matching-records
       '()))))

(defn find-sample-by-ids
  ([ids model member-model publication-frequency-model]
   (find-sample-by :id ids model member-model publication-frequency-model)))

(defn bulk-insert-sample
  [props model member-model publication-frequency-model]
  (let [now (f/unparse db-date-formatter (l/local-now))
        snake-cased-props (pmap snake-case-keys props)
        identified-props (pmap
                           #(assoc % :id (-> (uuid/v1) (uuid/v5 (:label %))))
                           snake-cased-props)
        timestamped-props (pmap
                            #(assoc % :created_at now)
                            identified-props)
        samples-ids (map #(:id %) timestamped-props)]
    (if (pos? (count timestamped-props))
      (do
        (try
          (db/insert model (db/values timestamped-props))
          (catch Exception e
            (error-handler/log-error e))))
        (find-sample-by-ids samples-ids model member-model publication-frequency-model))
      '()))