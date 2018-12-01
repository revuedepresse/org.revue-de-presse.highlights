(ns repository.archived-status
  (:require [korma.core :as db])
  (:use [korma.db]))

(declare archived-status)

(defn get-archived-status-model
  [connection]
  (db/defentity archived-status
                (db/table :weaving_archived_status)
                (db/database connection)
                (db/entity-fields
                  :ust_id
                  :ust_hash                                 ; sha1(str ust_text  ust_status_id)
                  :ust_text
                  :ust_full_name                            ; twitter user screen name
                  :ust_name                                 ; twitter user full name
                  :ust_access_token
                  :ust_api_document
                  :ust_created_at
                  :ust_status_id))
  archived-status)
