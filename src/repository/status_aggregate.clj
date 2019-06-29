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
        status-twitter-id-col (get-column "ust_status_id" status-model)
        text-col (get-column "ust_text" status-model)
        created-at-col (get-column "ust_created_at" status-model)
        status-api-document (get-column "ust_api_document" status-model)]
    (->
      (db/select* model)
      (db/fields [:status_id :status-id]
                 [:aggregate_id :aggregate-id]
                 [status-api-document :status-api-document]
                 [text-col :text]
                 [created-at-col :created-at]
                 [status-twitter-id-col :status-twitter-id]
                 [aggregate-name-col :aggregate-name]
                 [aggregate-screen-name-col :aggregate-screen-name-col]
                 [aggregate-screen-name-col :screen-name]
                 [aggregate-list-id-col :aggregate-list-id-col])
      (db/join aggregate-model (= aggregate-id-col :aggregate_id))
      (db/join status-model (= status-id-col :status_id)))))

(defn find-statuses-by-aggregate-name
  [aggregate-name {model           :status-aggregate
                   status-model    :status
                   aggregate-model :aggregate} & [limit]]
  (let [limit (if (some? limit)
                limit
                100)
        created-at-col (get-column "ust_created_at" status-model)
        aggregate-name-col (get-column "name" aggregate-model)
        matching-statuses (-> (select-fields model status-model aggregate-model)
                              (db/where {aggregate-name-col aggregate-name})
                              (db/order created-at-col "ASC")
                              (db/limit limit)
                              (db/select))]
    (if matching-statuses
      matching-statuses
      '())))