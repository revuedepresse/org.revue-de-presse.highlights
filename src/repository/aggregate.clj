(ns repository.aggregate
  (:require [clojure.tools.logging :as log]
            [korma.core :as db])
  (:use [korma.db]
        [utils.string]
        [repository.database-schema]
        [repository.query-executor]
        [twitter.status-hash]))

(declare aggregate status-aggregate)

(defn get-aggregate-model
  [connection]
  (db/defentity aggregate
                (db/table :weaving_aggregate)
                (db/database connection)
                (db/entity-fields
                  :id
                  :name
                  :created_at
                  :locked
                  :locked_at
                  :unlocked_at
                  :screen_name
                  :list_id))
  aggregate)

(defn get-status-aggregate-model
  [connection]
  (db/defentity status-aggregate
                (db/table :weaving_status_aggregate)
                (db/database connection)
                (db/entity-fields
                  :status_id
                  :aggregate_id))
  status-aggregate)

(defn find-aggregate-by-id
  "Find an aggregate by id"
  [id model]
  (->
    (db/select* model)
    (db/fields :id
               :name
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

(defn select-relationship-between-aggregate-and-status
  [model status-model]
  (let [status-id-col (get-column "ust_id" status-model)]
    (->
      (db/select* model)
      (db/fields [:status_id :status-id]
                 [:aggregate_id :aggregate-id]
                 [status-id-col :twitter-id])
      (db/join status-model (= status-id-col :status_id)))))

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
          (db/insert model (db/values snake-cased-values))
          (catch Exception e (log/error (.getMessage e))))
        (find-relationships-between-aggregate-and-statuses-having-ids
          aggregate-id
          statuses-ids
          model
          status-model))
      '())))

(defn get-member-aggregates-by-screen-name
  [screen-name]
  (let [query (str
                "SELECT                                                                      "
                "aggregate.id `aggregate-id`,                                                "
                "aggregate.name `aggregate-name`,                                            "
                "aggregate.screen_name as `member-name`,                                     "
                "aggregate.screen_name as `screen-name`,                                     "
                "subscription.last_status_publication_date as `last-status-publication-date` "
                "FROM member_subscription member_subscription,                               "
                "weaving_user member,                                                        "
                "weaving_user subscription,                                                  "
                "weaving_aggregate aggregate                                                 "
                "WHERE aggregate.screen_name = subscription.usr_twitter_username             "
                "AND aggregate.name like 'user :: %'                                         "
                "AND aggregate.screen_name IS NOT NULL                                       "
                "AND member_subscription.member_id = member.usr_id                           "
                "AND member_subscription.subscription_id = subscription.usr_id               "
                "AND subscription.suspended = 0                                              "
                "AND subscription.not_found = 0                                              "
                "AND subscription.protected = 0                                              "
                "AND member_subscription.has_been_cancelled = 0                              "
                "AND member.usr_twitter_username = ?                                         "
                "ORDER BY aggregate.name ASC                                                 ")
        results (db/exec-raw [query [screen-name]] :results)]
    results))

(defn get-aggregates-having-name-prefix
  [prefix]
  (let [query (str "
                SELECT
                member.usr_id as `member-id`,
                member.usr_twitter_username as `screen-name`,
                member.usr_twitter_username as `member-name`,
                a.id as `aggregate-id`,
                a.name as `aggregate-name`,
                member.last_status_publication_date as `last-status-publication-date`
                FROM (
                  SELECT
                  sa.aggregate_id,
                  GROUP_CONCAT(DISTINCT s.ust_full_name SEPARATOR ',') as member_names
                  FROM weaving_status_aggregate sa
                  INNER JOIN weaving_status s
                  ON s.ust_id = sa.status_id
                  WHERE aggregate_id IN (
                      SELECT
                      a.id as `aggregate-id`
                      FROM weaving_aggregate a
                      WHERE screen_name IS NULL
                      AND a.name NOT LIKE 'user ::%'
                      AND a.name LIKE ?
                  )
                  GROUP BY aggregate_id
                ) selection
                INNER JOIN weaving_user member
                ON FIND_IN_SET(member.usr_twitter_username, selection.member_names)
                AND member.protected = 0
                AND member.suspended = 0
                AND member.not_found = 0
                INNER JOIN weaving_aggregate a
                ON a.id = selection.aggregate_id
                ")
        results (db/exec-raw [query [(str prefix "%")]] :results)]
    results))

(defn get-member-aggregate
  [screen-name]
  (let [query (str
                "SELECT                                                                 "
                "aggregate.id `aggregate-id`,                                           "
                "aggregate.name `aggregate-name`                                        "
                "FROM                                                                   "
                "weaving_user member,                                                   "
                "weaving_aggregate aggregate                                            "
                "WHERE                                                                  "
                "member.usr_twitter_username = aggregate.screen_name                    "
                "AND aggregate.screen_name IS NOT NULL                                  "
                "AND aggregate.name = CONCAT('user :: ', member.usr_twitter_username)   "
                "AND member.usr_twitter_username = ?                                    ")
        results (exec-query [query [screen-name]] :results)]
    (first results)))