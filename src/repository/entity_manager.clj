(ns repository.entity-manager
    (:require [korma.core :as db])
    (:use [korma.db]))

(defn connect-to-db
  "Create a connection and provide with a map of entities"
  [config]
  (defdb database-connection {
                              :classname "com.mysql.jdbc.Driver"
                              :subprotocol "mysql"
                              :subname (str "//" (:host config) ":" (:port config) "/" (:name config))
                              :useUnicode "yes"
                              :characterEncoding (:charset config)
                              :delimiters "`"
                              :user (:user config)
                              :password (:password config)})

  (declare users)

  (db/defentity users
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields :usr_twitter_username :usr_twitter_id))

  {:users users})

(defn find-user-by-username
  "Find a user by her / his username"
  [username users]
  (-> (db/select* users)
      (db/fields [:usr_twitter_username :screen_name])
      (db/where {:usr_twitter_username username})
      (db/select)))
