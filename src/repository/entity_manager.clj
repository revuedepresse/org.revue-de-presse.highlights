(ns repository.entity-manager
    (:require [korma.core :as db]
              [clojure.edn :as edn]
              [clojure.tools.logging :as log]
              [clj-uuid :as uuid])
    (:use [korma.db]))

(defn connect-to-db
  "Create a connection and provide with a map of entities"
  [config]
  (defdb database-connection {:classname "com.mysql.jdbc.Driver"
                              :subprotocol "mysql"
                              :subname (str "//" (:host config) ":" (:port config) "/" (:name config))
                              :useUnicode "yes"
                              :characterEncoding (:charset config)
                              :delimiters "`"
                              :user (:user config)
                              :password (:password config)})

  (declare users members subscriptions subscribees member-subscriptions member-subscribees)

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
   :member-subscribees member-subscribees})

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

(defn find-member-subscription
  "Find a member subscription"
  [member-subscription member-subscriptions]
  (let [{member-id :member-id
         subscription-id :subscription-id} member-subscription
        member-subscription (-> (db/select* member-subscriptions)
                                 (db/fields :id
                                            :member_id
                                            :subscription_id)
                                 (db/where {:member_id member-id
                                            :subscription_id subscription-id})
                                 (db/select))]
    (first member-subscription)))

(defn select-members
  [members]
  (-> (db/select* members)
  (db/fields [:usr_id :id
              :usr_twitter_id :twitter-id
              :usr_twitter_username :screen_name])))

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

(defn find-members-ids-by-id
  "Find member by theirs Twitter ids"
  [twitter-ids members]
  (let [matching-members (find-members-by-id twitter-ids members)]
    (map #(:id %) matching-members)))

(defn new-member-subscription
    [member-subscription member-subscriptions]
    (let [{member-id :member-id
           subscription-id :subscription-id} member-subscription]
      (db/insert member-subscriptions
                 (db/values [{:id (uuid/to-string (uuid/v1))
                              :member_id member-id
                              :subscription_id subscription-id}]))
      (find-member-subscription {:member-id member-id
                                 :subscription-id subscription-id} member-subscriptions)))

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

      (db/insert members
                 (db/values [{:usr_twitter_id twitter-id
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
      (find-member-by-id twitter-id members)))

(defn ensure-member-subscription-exists
  "Ensure a member subscription exists"
  [member-subscription member-subscriptions]
  (let [{member-id :member-id
         subscription-id :subscription-id} member-subscription
         member-subscription (find-member-subscription {:member-id member-id
                                                        :subscription-id subscription-id}
                                                       member-subscriptions)]
    (if member-subscription
      (do (log/info (str "An existing subscription has been found in favor of member having id #" subscription-id))
      member-subscription)
      (do (log/info (str "A new subscription is about to be recorded in favor of member having id #" subscription-id))
      (new-member-subscription {:member-id member-id
                                :subscription-id subscription-id}
                               member-subscriptions)))))

(defn ensure-subscription-exists-in-favor-of-member-having-id
  [{member-id :member-id
    member-subscriptions :member-subscriptions}]
    (fn [subscription-id]
      (ensure-member-subscription-exists {:member-id member-id
                                          :subscription-id subscription-id}
                                         member-subscriptions)))

(defn ensure-subscriptions-exists-for-member-having-id
  [{member-id                          :member-id
    member-subscriptions               :member-subscriptions
    matching-subscriptions-members-ids :matching-subscriptions-members-ids}]
  (let [for-each-member-subscription (ensure-subscription-exists-in-favor-of-member-having-id {:member-id member-id
                                                                                               :member-subscriptions member-subscriptions})]
  (log/info (str "About to ensure " (count matching-subscriptions-members-ids)
                   " subscriptions for member having id #" member-id " are recorded."))
  (doall (map for-each-member-subscription matching-subscriptions-members-ids)))
)
