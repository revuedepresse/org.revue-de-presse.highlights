(ns repository.member-identity
  (:require [korma.core :as db])
  (:use [korma.db]
        [repository.database-schema]))

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

(defn select-fields
  [model]
  (->
    (db/select* model)
    (db/fields :id
               [:member_id :member-id]
               [:screen_name :member-name]
               [:twitter_id :member-twitter-id])))
