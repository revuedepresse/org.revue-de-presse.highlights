(ns repository.entity-manager
  (:require [korma.core :as db]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clj-uuid :as uuid])
  (:use [korma.db]
        [repository.aggregate]
        [repository.keyword]
        [repository.status]
        [repository.archived-status]
        [repository.database-schema]
        [repository.status-popularity]
        [repository.timely-status]
        [repository.highlight]
        [utils.string]
        [twitter.status-hash]))

(declare archive-database-connection database-connection
         tokens
         users members
         subscriptions subscribees
         member-subscriptions member-subscribees
         liked-status)

(defn get-liked-status-model
  [connection]
  (db/defentity liked-status
                (db/table :liked_status)
                (db/database connection)
                (db/entity-fields
                  :id
                  :status_id
                  :archived_status_id
                  :time_range
                  :is_archived_status
                  :member_id
                  :member_name
                  :liked_by
                  :liked_by_member_name
                  :aggregate_id
                  :aggregate_name))
  liked-status)

(defn get-token-model
  [connection]
  (db/defentity tokens
                (db/table :weaving_access_token)
                (db/database connection)
                (db/entity-fields
                  :token
                  :secret
                  :consumer_key
                  :consumer_secret
                  :frozen_until))
  tokens)

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

(defn get-members-model
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
                  :total_subscribees
                  :total_subscriptions))
  members)

(defn get-subscriptions-model
  [connection]
  (db/defentity subscriptions
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended))
  subscriptions)

(defn get-subscribees-model
  [connection]
  (db/defentity subscribees
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended))
  subscribees)

(defn get-member-subscriptions-model
  [connection]
  (db/defentity member-subscriptions
                (db/pk :id)
                (db/table :member_subscription)
                (db/database connection)
                (db/entity-fields :member_id :subscription_id)
                (db/has-one members {:fk :usr_id})
                (db/has-one subscriptions {:fk :usr_id}))
  member-subscriptions)

(defn get-member-subscribees-model
  [connection]
  (db/defentity member-subscribees
                (db/pk :id)
                (db/table :member_subscribee)
                (db/database connection)
                (db/entity-fields :member_id :subscribee_id)
                (db/has-one members {:fk :usr_id})
                (db/has-one subscribees {:fk :usr_id}))
  member-subscribees)

(defn prepare-connection
  [config & [is-archive-connection]]
  (let [db-params {:classname         "com.mysql.jdbc.Driver"
                   :subprotocol       "mysql"
                   :subname           (str "//" (:host config) ":" (:port config) "/" (:name config))
                   :useUnicode        "yes"
                   :characterEncoding "UTF-8"
                   :characterSet      "utf8mb4"
                   :collation         "utf8mb4_unicode_ci"
                   :delimiters        "`"
                   :useSSL            false
                   :user              (:user config)
                   :password          (:password config)
                   ; @see 'https://github.com/korma/Korma/issues/382#issue-236722546
                   :make-pool         true
                   :maximum-pool-size 5}
        connection (if is-archive-connection
                     (defdb archive-database-connection db-params)
                     (defdb database-connection db-params))]
    connection))

(defn connect-to-db
  "Create a connection and provide with a map of entities"
  ; @see https://mathiasbynens.be/notes/mysql-utf8mb4
  ; @see https://clojurians-log.clojureverse.org/sql/2017-04-05
  [config & [is-archive-connection]]
  (let [connection (prepare-connection config is-archive-connection)]
    {:aggregate            (get-aggregate-model connection)
     :archived-status      (get-archived-status-model connection)
     :highlight            (get-highlight-model connection)
     :hashtag              (get-keyword-model connection)
     :liked-status         (get-liked-status-model connection)
     :members              (get-members-model connection)
     :member-subscribees   (get-member-subscribees-model connection)
     :member-subscriptions (get-member-subscriptions-model connection)
     :subscribees          (get-subscribees-model connection)
     :status               (get-status-model connection)
     :status-aggregate     (get-status-aggregate-model connection)
     :status-popularity    (get-status-popularity-model connection)
     :subscriptions        (get-subscriptions-model connection)
     :timely-status        (get-timely-status-model connection)
     :tokens               (get-token-model connection)
     :users                (get-user-model connection)
     :connection           connection}))

(defn get-entity-manager
  [config & [is-archive-connection]]
  (connect-to-db (edn/read-string config) is-archive-connection))

(defn find-distinct-ids-of-subscriptions
  "Find distinct ids of subscription"
  []
  (let [results (db/exec-raw [(str "SELECT SQL_CACHE GROUP_CONCAT( "
                                   " DISTINCT subscription_id ORDER BY subscription_id DESC"
                                   " ) AS all_subscription_ids,"
                                   " COUNT(DISTINCT subscription_id) AS total_subscription_ids "
                                   "FROM member_subscription")] :results)
        all-subscriptions-ids (:all_subscription_ids (first results))
        total-subscription-ids (:total_subscription_ids (first results))
        subscriptions-ids (map
                            #(when % (Long/parseLong %))
                            (reverse
                              (conj (explode #"," all-subscriptions-ids) nil)))]
    (log/info (str "There are " (inc total-subscription-ids) " unique subscriptions ids."))
    subscriptions-ids))

(defn find-member-subscriptions
  "Find member subscription"
  [screen-name]
  (let [results (db/exec-raw [(str "SELECT SQL_CACHE                      "
                                   "member_id,                            "
                                   "GROUP_CONCAT(                         "
                                   "   FIND_IN_SET(                       "
                                   "     COALESCE(subscription_id, 0), (  "
                                   "       SELECT GROUP_CONCAT(           "
                                   "          DISTINCT subscription_id    "
                                   "       ) FROM member_subscription     "
                                   "     )                                "
                                   "   )                                  "
                                   " ) subscription_ids                   "
                                   " FROM member_subscription             "
                                   " WHERE member_id IN (                 "
                                   "   SELECT usr_id                      "
                                   "   FROM weaving_user                  "
                                   "   WHERE usr_twitter_username = ?     "
                                   " )                                    "
                                   " GROUP BY member_id") [screen-name]] :results)
        raw-subscriptions-ids (:subscription_ids (first results))
        subscriptions-ids (explode #"," raw-subscriptions-ids)]
    {:member-subscriptions  subscriptions-ids
     :raw-subscriptions-ids raw-subscriptions-ids}))

;; Create table containing total subscriptions per member
;
; CREATE TABLE tmp_subscriptions
; SELECT u.usr_id,
; COUNT(DISTINCT s.subscription_id) total_subscriptions
; FROM member_subscription s, weaving_user u
; WHERE u.usr_id = s.member_id GROUP BY member_id;

;; Add Index to temporary table
;
; ALTER TABLE `tmp_subscriptions` ADD INDEX `id` (`usr_id`, `total_subscriptions`);

;; Update member table
;
; UPDATE weaving_user u, tmp_subscriptions t
; SET u.total_subscriptions = t.total_subscriptions
; WHERE t.usr_id = u.usr_id;
; DROP table tmp_subscriptions;

(defn find-members-closest-to-member-having-screen-name
  [total-subscriptions]
  (let [min-subscriptions (* 0.5 total-subscriptions)
        max-subscriptions (* 1 total-subscriptions)
        params [min-subscriptions max-subscriptions]
        select-members-query (str
                               "SELECT SQL_CACHE                                     "
                               "u.usr_twitter_username identifier,                   "
                               "GROUP_CONCAT(                                        "
                               "  FIND_IN_SET(                                       "
                               "    subscription_id,                                 "
                               "    (SELECT group_concat(DISTINCT subscription_id)   "
                               "     FROM member_subscription)                       "
                               "    )                                                "
                               "  ) subscription_ids,                                "
                               "u.total_subscriptions                                "
                               "FROM member_subscription s, weaving_user u           "
                               "WHERE u.usr_id = s.member_id                         "
                               "AND total_subscriptions BETWEEN ? AND ?              "
                               "AND s.member_id in (                                 "
                               "   SELECT usr_id                                     "
                               "   FROM weaving_user                                 "
                               "   WHERE total_subscriptions > 0)                    "
                               "AND total_subscriptions > 0                          "
                               "GROUP BY member_id                                   "
                               "LIMIT 50                                             ")
        results (db/exec-raw [select-members-query params] :results)]
    results))

(defn count-members
  "Count the total number of members"
  []
  (let [results (db/exec-raw [(str "SELECT count(usr_id) AS total_members "
                                   "FROM weaving_user m")] :results)]
    (:total_members (first results))))

(defn find-single-statuses-per-member
  "Find a single status for each member"
  [start page-length & [fetch-archives]]
  (let [table-name (if fetch-archives "weaving_archived_status" "weaving_status")
        results (db/exec-raw [(str "SELECT "
                                   "s.ust_full_name AS `screen-name`, "
                                   "s.ust_api_document AS `api-document`, "
                                   "usr_id AS id "
                                   "FROM weaving_user m "
                                   "INNER JOIN " table-name " s "
                                   "ON s.ust_full_name = m.usr_twitter_username "
                                   "WHERE (m.url IS NULL or m.description IS NULL) "
                                   "LIMIT ?, ?") [start page-length]] :results)]
    results))

(defn update-member-description-and-url
  [{url         :url
    description :description
    id          :id} model]
  (db/update model
             (db/set-fields {:url         url
                             :description description})
             (db/where {:usr_id id})))

(defn find-liked-statuses
  [liked-statuses-ids model status-model]
  (let [status-id-col (get-column "ust_id" status-model)
        twitter-id-col (get-column "ust_status_id" status-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [twitter-id-col :twitter-id]
                 [:status_id :status-id]
                 [:archived_status_id :archived-status-id]
                 [:time_range :time-range]
                 [:is_archived_status :is-archived-status]
                 [:member_id :member-id]
                 [:member_name :member-name]
                 [:liked_by :liked-by]
                 [:liked_by_member_name :liked-by-member-name]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name])
      (db/join status-model (= status-id-col :status_id))
      (db/where {:id [in liked-statuses-ids]})
      (db/select))))

(defn new-liked-statuses
  [liked-statuses model status-model]
  (let [identified-liked-statuses (map #(assoc
                                          %
                                          :id
                                          (uuid/to-string (uuid/v1)))
                                       liked-statuses)
        liked-status-values (map snake-case-keys identified-liked-statuses)
        ids (map #(:id %) identified-liked-statuses)]
    (if (pos? (count ids))
      (do
        (try
          (db/insert model
                     (db/values liked-status-values))
          (catch Exception e (log/error (.getMessage e))))
        (find-liked-statuses ids model status-model))
      '())))

(defn update-min-favorite-id-for-member-having-id
  [min-favorite-id member-id model]
  (db/update model
             (db/set-fields {:min_like_id min-favorite-id})
             (db/where {:usr_id member-id})))

(defn update-max-favorite-id-for-member-having-id
  [max-favorite-id member-id model]
  (db/update model
             (db/set-fields {:max_like_id max-favorite-id})
             (db/where {:usr_id member-id})))

(defn update-min-status-id-for-member-having-id
  [min-status-id member-id model]
  (db/update model
             (db/set-fields {:min_status_id min-status-id})
             (db/where {:usr_id member-id})))

(defn update-max-status-id-for-member-having-id
  [max-status-id member-id model]
  (db/update model
             (db/set-fields {:max_status_id max-status-id})
             (db/where {:usr_id member-id})))

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
               [:usr_twitter_id :twitter-id]
               [:usr_twitter_username :screen-name]
               [:min_status_id :min-status-id]
               [:max_status_id :max-status-id]
               [:min_like_id :min-favorite-status-id]
               [:max_like_id :max-favorite-status-id])
    (db/where {:usr_twitter_username screen-name})
    (db/select)))

(defn select-members
  [members]
  (-> (db/select* members)
      (db/fields [:usr_id :id]
                 [:usr_twitter_id :twitter-id]
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

(defn select-tokens
  [model]
  (->
    (db/select* model)
    (db/fields [:consumer_key :consumer-key]
               [:consumer_secret :consumer-secret]
               [:frozen_until :frozen-until]
               :token
               :secret)))

(defn freeze-token
  [consumer-key]
  (db/exec-raw [(str "UPDATE weaving_access_token "
                     "SET frozen_until = DATE_ADD(NOW(), INTERVAL 15 MINUTE) "
                     "WHERE consumer_key = ?") [consumer-key]]))

(defn find-first-available-tokens
  "Find a token which has not been frozen"
  [model]
  (first (-> (select-tokens model)
             (db/where (and (= :type 1)
                            (not= (db/sqlfn coalesce :consumer_key -1) -1)
                            (<= :frozen_until (db/sqlfn now))))
             (db/select))))

(defn find-first-available-tokens-other-than
  "Find a token which has not been frozen"
  [consumer-keys model]
  (let [excluded-consumer-keys (if consumer-keys consumer-keys '("_"))
        first-available-token (first (-> (select-tokens model)
                                         (db/where (and
                                                     (= :type 1)
                                                     (not= (db/sqlfn coalesce :consumer_key -1) -1)
                                                     (not-in :consumer_key excluded-consumer-keys)
                                                     (<= :frozen_until (db/sqlfn now))))
                                         (db/order :frozen_until :ASC)
                                         (db/select)))]
    first-available-token))

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
  (let [ids (if subscriptions-ids subscriptions-ids '(0))]
    (-> (select-member-subscriptions model)
        (db/where (and (= :member_id member-id)
                       (in :subscription_id ids)))
        (db/select))))

(defn select-member-subscribees
  [model]
  (-> (db/select* model)
      (db/fields :id
                 :member_id
                 :subscribee_id)))

(defn find-member-subscribees-by
  "Find multiple member subscribees"
  [{:keys [member-id subscribees-ids]} model]
  (let [ids (if subscribees-ids subscribees-ids '(0))]
    (-> (select-member-subscribees model)
        (db/where (and (= :member_id member-id)
                       (in :subscribee_id ids)))
        (db/select))))

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

(defn create-member-subscription-values
  [member-id]
  (fn [subscription-id]
    {:id              (uuid/to-string (uuid/v1))
     :member_id       member-id
     :subscription_id subscription-id}))

(defn create-member-subscribee-values
  [member-id]
  (fn [subscribee-id]
    {:id            (uuid/to-string (uuid/v1))
     :member_id     member-id
     :subscribee_id subscribee-id}))

(defn ensure-subscriptions-exist-for-member-having-id
  [{member-id                          :member-id
    model                              :model
    matching-subscriptions-members-ids :matching-members-ids}]
  (let [existing-member-subscriptions (find-member-subscriptions-by {:member-id         member-id
                                                                     :subscriptions-ids matching-subscriptions-members-ids}
                                                                    model)
        existing-member-subscriptions-ids (map #(:subscription_id %) existing-member-subscriptions)
        member-subscriptions (clojure.set/difference (set matching-subscriptions-members-ids) (set existing-member-subscriptions-ids))
        missing-member-subscriptions (map (create-member-subscription-values member-id) member-subscriptions)]

    (when (pos? (count missing-member-subscriptions))
      (log/info (str "About to ensure " (count missing-member-subscriptions)
                     " subscriptions for member having id #" member-id " are recorded."))
      (new-member-subscriptions missing-member-subscriptions model)
      (log/info (str (count missing-member-subscriptions)
                     " subscriptions have been recorded successfully")))))

(defn ensure-subscribees-exist-for-member-having-id
  [{member-id                        :member-id
    model                            :model
    matching-subscribees-members-ids :matching-members-ids}]
  (let [existing-member-subscribees (find-member-subscribees-by {:member-id       member-id
                                                                 :subscribees-ids matching-subscribees-members-ids}
                                                                model)
        existing-member-subscribees-ids (map #(:subscribee_id %) existing-member-subscribees)
        member-subscribees (clojure.set/difference (set matching-subscribees-members-ids) (set existing-member-subscribees-ids))
        missing-member-subscribees (map (create-member-subscribee-values member-id) member-subscribees)]

    (when (pos? (count missing-member-subscribees))
      (log/info (str "About to ensure " (count missing-member-subscribees)
                     " subscribees for member having id #" member-id " are recorded."))
      (new-member-subscribees missing-member-subscribees model)
      (log/info (str (count missing-member-subscribees)
                     " subscribees have been recorded successfully")))))


