(ns repository.status-aggregate
  (:require [korma.core :as db])
  (:use [korma.db]
        [repository.database-schema]))

(declare status-aggregate)

(defn get-status-aggregate-model
  [connection]
  (db/defentity status-aggregate
                (db/table :weaving_status_aggregate)
                (db/database connection)
                (db/entity-fields
                  :status_id
                  :aggregate_id))
  status-aggregate)

(defn select-fields
  [model status-model aggregate-model]
  (let [aggregate-id-col (get-column "id" aggregate-model)
        aggregate-name-col (get-column "name" aggregate-model)
        aggregate-screen-name-col (get-column "screen_name" aggregate-model)
        aggregate-list-id-col (get-column "list_id" aggregate-model)
        status-id-col (get-column "ust_id" status-model)
        status-api-document (get-column "ust_api_document" status-model)]
    (->
      (db/select* model)
      (db/fields [:status_id :status-id]
                 [:aggregate_id :aggregate-id]
                 [status-api-document :status-api-document]
                 [aggregate-name-col :aggregate-name-col]
                 [aggregate-screen-name-col :aggregate-screen-name-col]
                 [aggregate-list-id-col :aggregate-list-id-col])
      (db/join aggregate-model (= aggregate-id-col :aggregate_id))
      (db/join status-model (= status-id-col :status_id)))))
