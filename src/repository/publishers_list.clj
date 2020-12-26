(ns repository.publishers-list
  (:require [korma.core :as db]
            [utils.error-handler :as error-handler])
  (:use [korma.db]
        [repository.database-schema]
        [repository.query-executor]
        [twitter.status-hash]
        [utils.string]))

(declare aggregate aggregate-subscription member-aggregate-subscription)

(defn get-aggregate-model
  [connection]
  (db/defentity aggregate
                (db/table :publishers_list)
                (db/database connection)
                (db/entity-fields
                  :id
                  :public_id
                  :name
                  :created_at
                  :locked
                  :locked_at
                  :unlocked_at
                  :screen_name
                  :list_id))
  aggregate)

(defn get-aggregate-subscription-model
  [connection]
  (db/defentity aggregate-subscription
                (db/table :aggregate_subscription)
                (db/database connection)
                (db/entity-fields
                  :id
                  :member_aggregate_subscription_id
                  :subscription_id))
  aggregate-subscription)

(defn get-member-aggregate-subscription-model
  [connection]
  (db/defentity member-aggregate-subscription
                (db/table :member_aggregate_subscription)
                (db/database connection)
                (db/entity-fields
                  :id
                  :member_id
                  :list_name
                  :list_id
                  :document))
  member-aggregate-subscription)

(defn find-aggregate-by-id
  "Find an aggregate by id"
  [id model]
  (->
    (db/select* model)
    (db/fields :id
               :name
               [:public_id :public-id]
               [:screen_name :screen-name])
    (db/where {:id id})
    (db/select)))

(defn find-aggregate-by-name
  "Find an aggregate by name"
  ([name model]
   (find-aggregate-by-name name false model))
  ([name include-member-aggregates model]
   (let [where {:name name}
         where (if include-member-aggregates
                 where
                 (assoc where :screen_name nil))]
     (->
       (db/select* model)
       (db/fields :id
                  :name
                  [:public_id :public-id]
                  [:screen_name :screen-name])
       (db/where where)
       (db/select)))))

(defn get-aggregate-by-id
  [aggregate-id model error-message]
  (let [matching-aggregates (find-aggregate-by-id aggregate-id model)
        aggregate (if (pos? (count matching-aggregates)) (first matching-aggregates) nil)
        _ (when (or
                  (nil? aggregate)
                  (nil? (:name aggregate)))
            (throw (Exception. (str error-message))))]
    aggregate))

(defn find-all-aggregates
  "Find all aggregates sorted by name"
  []
  (let [query (str "
                SELECT
                DISTINCT name as \"aggregate-name\",
                public_id as \"public-id\",
                id as \"aggregate-id\"
                FROM publishers_list
                WHERE screen_name IS NULL
                AND name NOT LIKE 'user ::%'
                ORDER BY name ASC")
        results (db/exec-raw [query] :results)]
    results))

(defn find-aggregates-enlisting-member
  "Find aggregates enlisting member"
  [screen-name]
  (let [query (str "
                SELECT
                DISTINCT name as \"aggregate-name\",
                public_id as \"public-id\",
                id as \"aggregate-id\"
                FROM publishers_list
                WHERE screen_name IS NOT NULL
                AND name NOT LIKE 'user ::%'
                AND screen_name = ?
                ORDER BY name ASC")
        results (db/exec-raw [query [screen-name]] :results)]
    results))

(defn find-keyword-aggregates
  "Find aggregates for which there are keywords"
  []
  (let [query (str "
                SELECT
                a.id as \"aggregate-id\",
                a.public_id as \"public-id\",
                a.name as \"aggregate-name\"
                FROM publishers_list a
                WHERE name NOT LIKE 'user ::%'
                AND screen_name IS NULL
                AND name IN (SELECT DISTINCT aggregate_name FROM keyword)
              ")
        results (db/exec-raw [query] :results)]
    results))

(defn find-members-by-aggregate
  "Find all aggregates sorted by name"
  [aggregate-name]
  (let [query (str "
                SELECT
                a.screen_name as \"screen-name\",
                a.list_id as \"aggregate-twitter-id\",
                mi.twitter_id as \"member-twitter-id\",
                mi.member_id as \"member-id\",
                a.name as \"aggregate-name\",
                a.public_id as \"aggregate-public-id\"
                FROM publishers_list a
                INNER JOIN member_identity mi
                ON mi.screen_name = a.screen_name
                WHERE a.screen_name IS NOT NULL
                AND a.name = ?
                AND list_id IS NOT NULL
                ORDER BY a.screen_name
              ")
        results (db/exec-raw [query [aggregate-name]] :results)]
    results))

(defn find-lists-of-subscriber-having-screen-name
  "Find members in lists by the screen name of a subscriber"
  [screen-name]
  (let [query (str "
                SELECT
                list_name AS \"list-name\",
                list_id AS \"list-twitter-id\"
                FROM member_aggregate_subscription
                WHERE member_id IN (
                    SELECT usr_id FROM weaving_user WHERE usr_twitter_username = ?
                ) ORDER BY list_name
              ")
        results (db/exec-raw [query [screen-name]] :results)]
    results))

(defn find-members-subscribing-to-lists
  "Find members subscribing to lists"
  []
  (let [query (str "
                SELECT DISTINCT usr_twitter_username AS \"screen-name\",
                usr_twitter_id AS \"member-twitter-id\",
                usr_id AS \"member-id\"
                FROM member_aggregate_subscription
                INNER JOIN weaving_user
                ON usr_id = member_id
                ORDER BY usr_twitter_username
              ")
        results (db/exec-raw [query] :results)]
    results))

(defn select-relationship-between-aggregate-and-status
  [model status-model]
  (let [twitter-status-id (get-column "ust_status_id" status-model)]
    (->
      (db/select* model)
      (db/fields [:status_id :status-id]
                 [:aggregate_id :aggregate-id]
                 [twitter-status-id :status-twitter-id])
      (db/join status-model (= twitter-status-id :status_id)))))

(defn find-relationships-between-aggregate-and-statuses-having-ids
  "Find relationships between an aggregate and statuses"
  [aggregate-id ids model status-model]
  (let [ids (if ids ids '(0))
        matching-relationships (-> (select-relationship-between-aggregate-and-status model status-model)
                                   (db/where (and
                                               (= :aggregate_id aggregate-id)
                                               (in :status_id ids)))
                                   (db/select))]
    (if (pos? (count matching-relationships))
      matching-relationships
      '())))

(defn bulk-insert-status-aggregate-relationship
  [relationships aggregate-id model status-model]
  (let [snake-cased-values (map snake-case-keys relationships)
        statuses-ids (map #(:status_id %) snake-cased-values)]
    (if (pos? (count snake-cased-values))
      (do
        (try
          (insert-query {:values snake-cased-values
                         :model  model})
          (catch Exception e
            (error-handler/log-error e)))
        (find-relationships-between-aggregate-and-statuses-having-ids
          aggregate-id
          statuses-ids
          model
          status-model))
      '())))

(defn get-member-aggregates-by-screen-name
  [screen-name]
  (let [query (str
                "SELECT                                                                        "
                "aggregate.id \"aggregate-id\",                                                "
                "aggregate.name \"aggregate-name\",                                            "
                "aggregate.public_id \"aggregate-public-id\",                                  "
                "aggregate.screen_name as \"member-name\",                                     "
                "aggregate.screen_name as \"screen-name\",                                     "
                "subscription.last_status_publication_date as \"last-status-publication-date\" "
                "FROM member_subscription member_subscription,                                 "
                "weaving_user member,                                                          "
                "weaving_user subscription,                                                    "
                "publishers_list aggregate                                                     "
                "WHERE aggregate.screen_name = subscription.usr_twitter_username               "
                "AND aggregate.name like 'user :: %'                                           "
                "AND aggregate.screen_name IS NOT NULL                                         "
                "AND member_subscription.member_id = member.usr_id                             "
                "AND member_subscription.subscription_id = subscription.usr_id                 "
                "AND subscription.suspended = 0                                                "
                "AND subscription.not_found = 0                                                "
                "AND subscription.protected = 0                                                "
                "AND member_subscription.has_been_cancelled = 0                                "
                "AND member.usr_twitter_username = ?                                           "
                "ORDER BY aggregate.name ASC                                                   ")
        results (db/exec-raw [query [screen-name]] :results)]
    results))

(defn select-aggregates-where
  [pattern & [additional-constraints]]
  (str "
      SELECT
      member.usr_id as \"member-id\",
      member.usr_twitter_username as \"screen-name\",
      member.usr_twitter_username as \"member-name\",
      a.id as \"aggregate-id\",
      a.name as \"aggregate-name\",
      a.public_id as \"aggregate-public-id\",
      member.last_status_publication_date as \"last-status-publication-date\"
      FROM member_aggregate_subscription msub
      INNER JOIN publishers_list a
      ON list_name = name
      " additional-constraints "
      INNER JOIN aggregate_subscription asub
      ON asub.member_aggregate_subscription_id = msub.id
      INNER JOIN weaving_user member
      ON member.usr_twitter_id = subscription_id
      WHERE list_name " pattern "
    "))

(defn get-aggregates-having-name-prefix
  [prefix]
  (let [query (select-aggregates-where
                "LIKE ?"
                "AND a.screen_name IS NULL")
        results (db/exec-raw [query [(str prefix "%")]] :results)]
    results))

(defn get-aggregates-sharing-name
  [name]
  (let [query (select-aggregates-where
                "= ?"
                (str "AND a.screen_name IS NOT NULL"))
        results (db/exec-raw [query [name]] :results)]
    results))

(defn get-member-aggregate
  [screen-name]
  (let [query (str "
                SELECT
                aggregate.id \"aggregate-id\",
                aggregate.public_id \"aggregate-public-id\",
                aggregate.name \"aggregate-name\"
                FROM
                weaving_user member,
                publishers_list aggregate
                WHERE
                member.usr_twitter_username = aggregate.screen_name " (get-collation) "
                AND aggregate.screen_name IS NOT NULL
                AND aggregate.name = CONCAT(
                  'user :: ', member.usr_twitter_username
                )
                AND member.usr_twitter_username = ?
              ")
        results (exec-query [query [screen-name]] :results)]
    (first results)))

(defn find-aggregates-sharing-name
  [aggregate-name db]
  (let [query (str "
                SELECT
                id as \"aggregate-id\",
                name as \"aggregate-name\",
                public_id as \"aggregate-public-id\",
                screen_name as \"screen-name\"
                FROM publishers_list a
                WHERE
                a.name = ?
                AND a.list_id IS NOT NULL
                AND a.screen_name IS NOT NULL;
              ")]
    (binding [*current-db* db]
      (exec-query [query [aggregate-name]] :results))))

(defn new-relationship
  [aggregate-id]
  (fn [status-id]
    (let [relationship {:status-id status-id :aggregate-id aggregate-id}]
      relationship)))
