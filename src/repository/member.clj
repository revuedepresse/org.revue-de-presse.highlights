(ns repository.member
  (:require [clojure.tools.logging :as log]
            [korma.core :as db])
  (:use [korma.db]
        [repository.database-schema]
        [utils.string]))


(declare members users)

(defn get-member-model
  [connection]
  (db/defentity members
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended
                  :usr_status
                  :description
                  :url
                  :last_status_publication_date
                  :max_like_id
                  :min_like_id
                  :total_subscribees
                  :total_subscriptions))
  members)

(defn get-user-model
  [connection]
  ; It seems that duplicates are required to express the relationships
  ; @see https://github.com/korma/Korma/issues/281
  (db/defentity users
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :usr_email
                  :not_found
                  :protected
                  :suspended))
  users)

(defn count-members
  "Count the total number of members"
  []
  (let [results (db/exec-raw [(str "SELECT count(usr_id) AS total_members "
                                   "FROM weaving_user m")] :results)]
    (:total_members (first results))))

(defn find-member-by-twitter-id
  "Find a member by her / his twitter id"
  [id members]
  (->
    (db/select* members)
    (db/fields [:usr_id :id]
               [:usr_twitter_id :twitter-id]
               [:usr_twitter_username :screen-name])
    (db/where {:usr_twitter_id id})
    (db/select)))

(defn find-member-by-screen-name
  "Find a member by her / his username"
  [screen-name members]
  (->
    (db/select* members)
    (db/fields [:usr_id :id]
               [:usr_id :member-id]
               [:usr_twitter_id :twitter-id]
               [:usr_twitter_id :member-twitter-id]
               [:usr_twitter_username :screen-name]
               [:description :description]
               [:min_status_id :min-status-id]
               [:max_status_id :max-status-id]
               [:min_like_id :min-favorite-status-id]
               [:max_like_id :max-favorite-status-id])
    (db/where {:usr_twitter_username screen-name})
    (db/order :usr_twitter_username "ASC")
    (db/select)))

(defn find-member-by-twitter-id
  "Find a member by her / his twitter id"
  [id members]
  (->
    (db/select* members)
    (db/fields [:usr_id :id]
               [:usr_twitter_id :twitter-id]
               [:usr_twitter_username :screen-name])
    (db/where {:usr_twitter_id id})
    (db/select)))

(defn select-members
  [members]
  (-> (db/select* members)
      (db/fields [:usr_id :id]
                 [:usr_twitter_id :twitter-id]
                 [:description :description]
                 [:usr_twitter_username :screen_name]
                 [:usr_twitter_username :screen-name])))

(defn find-member-by-id
  "Find a member by her / his Twitter id"
  [twitter-id members]
  (let [matching-members (-> (select-members members)
                             (db/where {:usr_twitter_id twitter-id})
                             (db/select))]
    (first matching-members)))

(defn find-members-having-ids
  "Find members by their Twitter ids"
  [twitter-ids members]
  (let [ids (if twitter-ids twitter-ids '(0))
        matching-members (-> (select-members members)
                             (db/where {:usr_twitter_id [in ids]})
                             (db/select))]
    (if matching-members
      matching-members
      '())))

(defn screen-name-otherwise-twitter-id
  [{is-protected :is-protected
    is-suspended :is-suspended
    is-not-found :is-not-found
    screen-name  :screen-name
    twitter-id   :twitter-id}]
  (if (and
        (or is-not-found is-protected is-suspended)
        (not screen-name))
    twitter-id
    screen-name))

(defn new-member
  [member members]
  (let [{id                  :id
         twitter-id          :twitter-id
         description         :description
         url                 :url
         total-subscribees   :total-subscribees
         total-subscriptions :total-subscriptions
         is-protected        :is-protected
         is-suspended        :is-suspended
         is-not-found        :is-not-found} member
        member-screen-name (screen-name-otherwise-twitter-id member)]

    (cond
      (= 1 is-protected)
      (log/info (str "About to cache protected member with twitter id #" twitter-id))
      (= 1 is-not-found)
      (log/info (str "About to cache not found member with twitter id #" twitter-id))
      (= 1 is-suspended)
      (log/info (str "About to cache suspended member with twitter id #" twitter-id))
      :else
      (log/info (str "About to cache member with twitter id #" twitter-id
                     " and twitter screen mame \"" member-screen-name "\"")))

    (try
      (if id
        (db/update members
                   (db/set-fields {:not_found is-not-found
                                   :suspended is-suspended
                                   :protected is-protected})
                   (db/where {:usr_id id}))
        (db/insert members
                   (db/values [{:usr_position_in_hierarchy 1 ; to discriminate test user from actual users
                                :usr_twitter_id            twitter-id
                                :usr_twitter_username      member-screen-name
                                :usr_status                false
                                :usr_email                 (str "@" member-screen-name)
                                :description               description
                                :url                       url
                                :not_found                 is-not-found
                                :suspended                 is-suspended
                                :protected                 is-protected
                                :total_subscribees         total-subscribees
                                :total_subscriptions       total-subscriptions}])))
      (catch Exception e (log/error (.getMessage e))))

    (find-member-by-id twitter-id members)))

(defn normalize-columns
  [members]
  (let [snake-cased-values (map snake-case-keys members)
        members-values (map
                         #(dissoc
                            (assoc
                              %
                              :usr_position_in_hierarchy 1
                              :usr_status false
                              :usr_email (str "@" (:screen_name %))
                              :usr_twitter_username (:screen_name %)
                              :usr_twitter_id (:twitter_id %)
                              :not_found (:is_not_found %)
                              :protected (:is_protected %)
                              :suspended (:is_suspended %))
                            :screen_name
                            :twitter_id
                            :is_not_found
                            :is_protected
                            :is_suspended)
                         snake-cased-values)
        deduped-values (dedupe (sort-by #(:usr_twitter_id %) members-values))
        twitter-ids (map #(:usr_twitter_id %) deduped-values)]
    {:deduped-values deduped-values
     :members-values members-values
     :twitter-ids    twitter-ids}))

(defn find-members-from-props
  [members model]
  (let [{members-values :members-values
         twitter-ids    :twitter-ids} (normalize-columns members)]
    (if (pos? (count members-values))
      (find-members-having-ids twitter-ids model))
    '()))

(defn bulk-insert-new-members
  [members model]
  (let [{deduped-values :deduped-values
         members-values :members-values
         twitter-ids    :twitter-ids} (normalize-columns members)]
    (if (pos? (count members-values))
      (do
        (try
          (db/insert model (db/values deduped-values))
          (catch Exception e (log/error (.getMessage e))))
        (find-members-having-ids twitter-ids model))
      '())))
