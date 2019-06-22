(ns repository.status-identity
  (:require [korma.core :as db])
  (:use [korma.db]))

(declare status-identity)

(defn get-status-identity-model
  [connection]
  (db/defentity status-identity
                (db/table :status_identity)
                (db/database connection)
                (db/entity-fields
                  :id
                  :member_identity
                  :twitter_id
                  :publication_date_time))
  status-identity)
