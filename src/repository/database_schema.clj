(ns repository.database-schema
  (:require [korma.core :as db])
  (:use [korma.db]))

(defn get-column
  [column-name model]
  (keyword (str (:table model) "." column-name)))

(defn get-model
  [{connection :connection
    model :model
    table :table
    fields :fields}]
  (let [fields (apply db/entity-fields fields)]
    (db/defentity model
                  (db/pk :id)
                  (db/table table)
                  (db/database connection)
                  fields)
    model))
