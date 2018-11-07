(ns repository.entity-manager
    (:require [korma.core :as db]
              [clojure.edn :as edn]
              [clojure.tools.logging :as log]
              [clj-uuid :as uuid])
    (:use [korma.db]))

(defn connect-to-db
  "Create a connection and provide with a map of entities"
  ; @see https://mathiasbynens.be/notes/mysql-utf8mb4
  ; @see https://clojurians-log.clojureverse.org/sql/2017-04-05
  [config]
  (defdb database-connection {:classname "com.mysql.jdbc.Driver"
                              :subprotocol "mysql"
                              :subname (str "//" (:host config) ":" (:port config) "/" (:name config))
                              :useUnicode "yes"
                              :characterEncoding "UTF-8"
                              :characterSet "utf8mb4"
                              :collation "utf8mb4_unicode_ci"
                              :delimiters "`"
                              :user (:user config)
                              :password (:password config)})

  (declare tokens users members subscriptions subscribees member-subscriptions member-subscribees)

  (db/defentity tokens
                (db/table :weaving_access_token)
                (db/database database-connection)
                (db/entity-fields
                  :token
                  :secret
                  :consumer_key
                  :consumer_secret
                  :frozen_until))

  ; It seems that duplicates are required to express the relationships
  ; @see https://github.com/korma/Korma/issues/281
  (db/defentity users
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :usr_email
                  :not_found
                  :protected
                  :suspended))

  (db/defentity members
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended
                  :usr_locked
                  :usr_status
                  :description
                  :total_subscribees
                  :total_subscriptions))

  (db/defentity subscriptions
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended))

  (db/defentity subscribees
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended))

  (db/defentity member-subscriptions
                (db/pk :id)
                (db/table :member_subscription)
                (db/database database-connection)
                (db/entity-fields :member_id :subscription_id)
                (db/has-one members {:fk :usr_id})
                (db/has-one subscriptions {:fk :usr_id}))

  (db/defentity member-subscribees
                (db/pk :id)
                (db/table :member_subscribee)
                (db/database database-connection)
                (db/entity-fields :member_id :subscribee_id)
                (db/has-one members {:fk :usr_id})
                (db/has-one subscribees {:fk :usr_id}))

  {:users users
   :members members
   :subscribees subscribees
   :subscriptions subscriptions
   :member-subscriptions member-subscriptions
   :member-subscribees member-subscribees
   :tokens tokens})

(defn get-entity-manager
  [config]
  (connect-to-db (edn/read-string config)))

(defn find-member-by-screen-name
  "Find a member by her / his username"
  [screen-name members]
  (-> (db/select* members)
      (db/fields [:usr_id :id]
                 [:usr_twitter_id :twitter-id]
                 [:usr_twitter_username :screen-name])
      (db/where {:usr_twitter_username screen-name})
      (db/select)))

(defn select-tokens
  [model]
  (-> (db/select* model)
      (db/fields [:consumer_key :consumer-key]
                 [:consumer_secret :consumer-secret]
                 :token
                 :secret)))

(defn find-first-available-tokens
  "Find a token which has not been frozen"
  [model]
  (second (-> (select-tokens model)
      (db/where (and (= :type 1)
                      (not= (db/sqlfn coalesce :consumer_key -1) -1)
                      (<= :frozen_until (db/sqlfn now))))
      (db/select))))

(defn select-member-subscriptions
  [model]
  (-> (db/select* model)
      (db/fields :id
                 :member_id
                 :subscription_id)))

(defn find-member-subscriptions-by
  "Find multiple member subscriptions"
  ; @see https://clojure.org/guides/destructuring#_where_to_destructure
  [{:keys [member-id subscriptions-ids]} model]
  (-> (select-member-subscriptions model)
                                 (db/where (and (= :member_id member-id)
                                                (in :subscription_id subscriptions-ids)))
                                 (db/select)))

(defn select-member-subscribees
  [model]
  (-> (db/select* model)
      (db/fields :id
                 :member_id
                 :subscribee_id)))

(defn find-member-subscribees-by
  "Find multiple member subscribees"
  [{:keys [member-id subscribees-ids]} model]
  (-> (select-member-subscribees model)
                                 (db/where (and (= :member_id member-id)
                                                (in :subscribee_id subscribees-ids)))
                                 (db/select)))

(defn select-members
  [members]
  (-> (db/select* members)
  (db/fields [:usr_id :id]
             [:usr_twitter_id :twitter-id]
             [:usr_twitter_username :screen_name])))

(defn find-member-by-id
  "Find a member by her / his Twitter id"
  [twitter-id members]
  (let [matching-members (-> (select-members members)
                                 (db/where {:usr_twitter_id twitter-id})
                                 (db/select))]
    (first matching-members)))

(defn find-members-by-id
  "Find member by theirs Twitter ids"
  [twitter-ids members]
  (let [matching-members (-> (select-members members)
                                 (db/where {:usr_twitter_id [in twitter-ids]})
                                 (db/select))]
    (if matching-members
      matching-members
      '())))

(defn map-get-in
  "Return a map of values matching the provided key coerced to integers"
  ; @see https://stackoverflow.com/a/31846875/282073
  [k coll]
  (let [coerce #(if (number? %) % (Long/parseLong %))
        get-val #(get-in % [k])
        get-coerced #(try (-> % get-val coerce)
         (catch Exception e (log/error (.getMessage e))))]
     (map get-coerced coll)))

(defn deduce-ids-of-missing-members
  [matching-members ids]
  (let [matching-ids (map-get-in :twitter-id matching-members)]
    (clojure.set/difference (set ids) (set matching-ids))))

(defn new-member-subscriptions
  [member-subscriptions model]
  (db/insert model (db/values member-subscriptions)))

(defn new-member-subscribees
  [member-subscribees model]
  (db/insert model (db/values member-subscribees)))

(defn new-member
  [member members]
  (let [{screen-name :screen-name
         twitter-id :twitter-id
         description :description
         total-subscribees :total-subscribees
         total-subscriptions :total-subscriptions
         is-protected :is-protected
         is-suspended :is-suspended
         is-not-found :is-not-found} member]
    (try
      (db/insert members
               (db/values [{:usr_position_in_hierarchy 1    ; to discriminate test user from actual users
                            :usr_twitter_id twitter-id
                            :usr_twitter_username screen-name
                            :usr_locked false
                            :usr_status false
                            :usr_email (str "@" screen-name)
                            :description description
                            :not_found is-not-found
                            :suspended is-suspended
                            :protected is-protected
                            :total_subscribees total-subscribees
                            :total_subscriptions total-subscriptions}]))
      (catch Exception e (log/error (.getMessage e))))
    (find-member-by-id twitter-id members)))

(defn create-member-subscription-values
  [member-id]
  (fn [subscription-id]
    {:id              (uuid/to-string (uuid/v1))
     :member_id       member-id
     :subscription_id subscription-id}))

(defn create-member-subscribee-values
  [member-id]
  (fn [subscribee-id]
    {:id              (uuid/to-string (uuid/v1))
     :member_id       member-id
     :subscribee_id subscribee-id}))

(defn ensure-subscriptions-exist-for-member-having-id
  [{member-id :member-id
    model     :model
    matching-subscriptions-members-ids :matching-subscriptions-members-ids}]
  (let [existing-member-subscriptions (find-member-subscriptions-by {:member-id         member-id
                                                                     :subscriptions-ids matching-subscriptions-members-ids}
                                                                    model)
        existing-member-subscriptions-ids (map #(:subscription_id %) existing-member-subscriptions)
        member-subscriptions (clojure.set/difference (set matching-subscriptions-members-ids) (set existing-member-subscriptions-ids))
        missing-member-subscriptions (map (create-member-subscription-values member-id) member-subscriptions)]

    (log/info (str "About to ensure " (count missing-member-subscriptions)
                   " subscriptions for member having id #" member-id " are recorded."))

    (new-member-subscriptions missing-member-subscriptions model)

    (log/info (str (count missing-member-subscriptions)
                 " subscriptions have been recorded successfully"))))

(defn ensure-subscribees-exist-for-member-having-id
  [{member-id :member-id
    model     :model
    matching-subscribees-members-ids :matching-subscribees-members-ids}]
  (let [existing-member-subscribees (find-member-subscribees-by {:member-id member-id
                                                                 :subscribees-ids matching-subscribees-members-ids}
                                                                model)
        existing-member-subscribees-ids (map #(:subscribee_id %) existing-member-subscribees)
        member-subscribees (clojure.set/difference (set matching-subscribees-members-ids) (set existing-member-subscribees-ids))
        missing-member-subscribees (map (create-member-subscribee-values member-id) member-subscribees)]

    (log/info (str "About to ensure " (count missing-member-subscribees)
                   " subscribees for member having id #" member-id " are recorded."))

    (new-member-subscribees missing-member-subscribees model)

    (log/info (str (count missing-member-subscribees)
                 " subscribees have been recorded successfully"))))


