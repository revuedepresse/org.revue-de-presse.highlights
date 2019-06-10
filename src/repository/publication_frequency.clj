(ns repository.publication-frequency
  (:require [korma.core :as db]
            [clj-uuid :as uuid]
            [clojure.tools.logging :as log])
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
                  :per_hour_of_day
                  :per_day_of_week
                  :updated_at))
  publication-frequency)

(defn select-publication-frequencies
  [model]
  (->
    (db/select* model)
    (db/fields [:id]
               [:member_id :member-id]
               [:per_hour_of_day :per-hour-of-day]
               [:per_day_of_week :per-day-of-week]
               [:updated_at :updated-at])))

(defn find-publication-frequencies-having-column-matching-values
  "Find frequencies of publication per day of week and hour or day which values of a given column
  can be found in collection passed as argument"
  ([column values model]
   (let [values (if values values '(0))
         matching-records (-> (select-publication-frequencies model)
                              (db/where {column [in values]})
                              (db/select))]
     (if matching-records
       matching-records
       '()))))

(defn find-publication-frequencies-by-ids
  [ids model]
  (find-publication-frequencies-having-column-matching-values :id ids model))

(defn bulk-insert-from-props
  [props model]
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
          (catch Exception e (log/error (.getMessage e))))
        (find-publication-frequencies-by-ids publication-frequencies-ids model))
      '())))