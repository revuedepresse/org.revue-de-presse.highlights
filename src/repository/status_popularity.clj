(ns repository.status-popularity
  (:require [korma.core :as db]
            [clj-uuid :as uuid]
            [utils.error-handler :as error-handler])
  (:use [korma.db]
        [utils.string]))

(declare status-popularity)

(defn get-status-popularity-model
  [connection]
  (db/defentity status-popularity
                (db/pk :id)
                (db/table :status_popularity)
                (db/database connection)
                (db/entity-fields
                  :checked_at
                  :status_id
                  :total_retweets
                  :total_favorites))
  status-popularity)

(defn select-statuses-popularities
  [model]
  (->
    (db/select* model)
    (db/fields [:id]
               [:checked_at :checked-at]
               [:status_id :status-id]
               [:total_retweets :total-retweets]
               [:total_favorites :total-favorites])))

(defn find-statuses-popularity-having-column-matching-values
  "Find popularity of statuses which values of a given column
  can be found in collection passed as argument"
  ([column values model]
    (let [values (if values values '(0))
          matching-records (-> (select-statuses-popularities model)
                                (db/where {column [in values]})
                                (db/select))]
      (if matching-records
        matching-records
        '())))
  ([column values date model]
   (let [values (if values values '(0))
         matching-records (->
                            (select-statuses-popularities model)
                              (db/where (and
                                          (= :checked_at date)
                                          {column [in values]}))
                              (db/select))]
     (if matching-records
       matching-records
       '()))))

(defn find-status-popularity-by-status-ids
  [ids model]
  (find-statuses-popularity-having-column-matching-values :id ids model))

(defn find-status-popularity-by-status-ids-and-date
  [statuses-ids date model]
  (find-statuses-popularity-having-column-matching-values :status_id statuses-ids date model))

(defn bulk-insert-of-status-popularity-props
  [status-popularity-props checked-at model]
  (let [identified-props (pmap
                           #(assoc % :id (-> (uuid/v1) (uuid/v5 (:status-id %))))
                           status-popularity-props)
        statuses-ids (map #(:status-id %) identified-props)
        existing-statuses (find-status-popularity-by-status-ids-and-date statuses-ids checked-at model)
        existing-statuses-ids (dedupe (map #(:status-id %) existing-statuses))
        new-records (remove #(clojure.set/subset? #{(:status-id %)} (set existing-statuses-ids)) identified-props)
        snake-cased-props (map snake-case-keys new-records)
        statuses-popularities-ids (pmap #(:id %) snake-cased-props)]
    (if (not-empty statuses-popularities-ids)
      (do
        (try
          (db/insert model (db/values snake-cased-props))
          (catch Exception e
            (error-handler/log-error e)))
        (find-status-popularity-by-status-ids statuses-popularities-ids model))
      '())))