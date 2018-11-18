(ns repository.aggregate
  (:require [clojure.tools.logging :as log]
            [korma.core :as db])
  (:use [korma.db]
        [utils.string]
        [repository.database-schema]
        [twitter.status-hash]))

(declare aggregate status-aggregate)

(defn get-aggregate-model
  [connection]
  (db/defentity aggregate
                (db/table :weaving_aggregate)
                (db/database connection)
                (db/entity-fields
                  :id
                  :name
                  :created_at
                  :locked
                  :locked_at
                  :unlocked_at
                  :screen_name
                  :list_id))
  aggregate)

(defn get-status-aggregate-model
  [connection]
  (db/defentity status-aggregate
                (db/table :weaving_status_aggregate)
                (db/database connection)
                (db/entity-fields
                  :status_id
                  :aggregate_id))
  status-aggregate)

(defn find-aggregate-by-id
  "Find an aggregate by id"
  [id model]
  (->
    (db/select* model)
    (db/fields :id
               :name
               [:screen_name :screen-name])
    (db/where {:id id})
    (db/select)))

(defn get-aggregate-by-id
  [aggregate-id model error-message]
  (let [aggregate (first (find-aggregate-by-id aggregate-id model))
        _ (when (nil? (:name aggregate))
            (throw (Exception. (str error-message))))]
    aggregate))

(defn select-relationship-between-aggregate-and-status
  [model status-model]
  (let [status-id-col (get-column "ust_id" status-model)]
    (->
      (db/select* model)
      (db/fields [:status_id :status-id]
                 [:aggregate_id :aggregate-id]
                 [status-id-col :twitter-id])
      (db/join status-model (= status-id-col :status_id)))))

(defn find-relationships-between-aggregate-and-statuses-having-ids
  "Find relationships between an aggregate and statuses"
  [aggregate-id ids model status-model]
  (let [ids (if ids ids '(0))
        matching-relationships (-> (select-relationship-between-aggregate-and-status model status-model)
                              (db/where (and
                                          (= :aggregate_id aggregate-id)
                                          (in :status_id ids)))
                              (db/select))]
    (if (pos? (count matching-relationships))
      matching-relationships
      '())))

(defn bulk-insert-status-aggregate-relationship
  [relationships aggregate-id model status-model]
  (let [snake-cased-values (map snake-case-keys relationships)
        statuses-ids (map #(:status_id %) snake-cased-values)]
    (if (pos? (count snake-cased-values))
      (do
        (try
          (db/insert model (db/values snake-cased-values))
          (catch Exception e (log/error (.getMessage e))))
        (find-relationships-between-aggregate-and-statuses-having-ids
          aggregate-id
          statuses-ids
          model
          status-model))
      '())))