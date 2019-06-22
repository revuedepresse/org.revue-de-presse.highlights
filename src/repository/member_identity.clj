(ns repository.member-identity
  (:require [korma.core :as db])
  (:use [korma.db]))

(declare member-identity)

(defn get-member-identity-model
  [connection]
  (db/defentity member-identity
                (db/table :member_identity)
                (db/database connection)
                (db/entity-fields
                  :id
                  :member_id
                  :twitter_id
                  :screen_name))
  member-identity)
