(ns repository.entity-manager
  (:require [korma.core :as db]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clj-uuid :as uuid]
            [repository.aggregate :as aggregate]
            [repository.analysis.sample :as sample]
            [repository.analysis.publication-frequency :as publication-frequency]
            [repository.archived-status :as archived-status]
            [repository.keyword :as keyword]
            [repository.member :as member]
            [repository.member-identity :as member-identity]
            [repository.member-subscription :as member-subscription]
            [repository.publication :as publication]
            [repository.highlight :as highlight]
            [repository.status :as status]
            [repository.status-aggregate :as status-aggregate]
            [repository.status-identity :as status-identity]
            [repository.status-popularity :as status-popularity]
            [repository.timely-status :as timely-status]
            [utils.error-handler :as error-handler])
  (:use [korma.db]
        [repository.database-schema]
        [utils.string]
        [twitter.status-hash]))

(declare archive-database-connection database-connection database-read-connection
         tokens
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

(defn prepare-connection
  [config & [{is-archive-connection :is-archive-connection
              is-read-connection    :is-read-connection}]]
  (let [db-params {:classname         "com.mysql.jdbc.Driver"
                   :subprotocol       "mysql"
                   :subname           (str "//"
                                           (:host config) ":" (:port config) "/"
                                           ; @see https://stackoverflow.com/a/39095756/282073
                                           (:name config) "?zeroDateTimeBehavior=convertToNull&autoReconnect=true")
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
        connection (cond
                     (some? is-archive-connection) (defdb archive-database-connection db-params)
                     (some? is-read-connection) (defdb database-read-connection db-params)
                     :else (defdb database-connection db-params))]
    connection))

(defn connect-to-db
  "Create a connection and provide with a map of entities"
  ; @see https://mathiasbynens.be/notes/mysql-utf8mb4
  ; @see https://clojurians-log.clojureverse.org/sql/2017-04-05
  [config & [{is-archive-connection :is-archive-connection
              is-read-connection    :is-read-connection}]]
  (let [connection (prepare-connection config {:is-archive-connection is-archive-connection
                                               :is-read-connection    is-read-connection})]
    {:aggregate             (aggregate/get-aggregate-model connection)
     :archived-status       (archived-status/get-archived-status-model connection)
     :highlight             (highlight/get-highlight-model connection)
     :hashtag               (keyword/get-keyword-model connection)
     :keyword               (keyword/get-keyword-model connection)
     :liked-status          (get-liked-status-model connection)
     :members               (member/get-member-model connection)
     :member                (member/get-member-model connection)
     :member-identity       (member-identity/get-member-identity-model connection)
     :member-subscribees    (member-subscription/get-member-subscribees-model connection)
     :member-subscriptions  (member-subscription/get-member-subscriptions-model connection)
     :publication           (publication/get-publication-model connection)
     :publication-frequency (publication-frequency/get-publication-frequency-model connection)
     :sample                (sample/get-sample-model connection)
     :subscribees           (member-subscription/get-subscribees-model connection)
     :status                (status/get-status-model connection)
     :status-aggregate      (status-aggregate/get-status-aggregate-model connection)
     :status-identity       (status-identity/get-status-identity-model connection)
     :status-popularity     (status-popularity/get-status-popularity-model connection)
     :subscriptions         (member-subscription/get-subscriptions-model connection)
     :timely-status         (timely-status/get-timely-status-model connection)
     :tokens                (get-token-model connection)
     :users                 (member/get-user-model connection)
     :connection            connection}))

(defn get-entity-manager
  [config & [{is-archive-connection :is-archive-connection
              is-read-connection    :is-read-connection}]]
  (connect-to-db (edn/read-string config) {:is-archive-connection is-archive-connection
                                           :is-read-connection    is-read-connection}))

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
          (catch Exception e
            (error-handler/log-error e)))
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

(defn update-status-related-props-for-member-having-id
  [max-status-id max-status-publication-date member-id model]
  (db/update model
             (db/set-fields {:max_status_id                max-status-id
                             :last_status_publication_date max-status-publication-date})
             (db/where {:usr_id member-id})))

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

(defn map-get-in
  "Return a map of values matching the provided key coerced to integers"
  ; @see https://stackoverflow.com/a/31846875/282073
  [k coll]
  (let [coerce #(if (number? %) % (Long/parseLong %))
        get-val #(get-in % [k])
        get-coerced #(try (-> % get-val coerce)
                          (catch Exception e
                            (error-handler/log-error e)))]
    (map get-coerced coll)))

(defn deduce-ids-of-missing-members
  [matching-members ids]
  (let [matching-ids (map-get-in :twitter-id matching-members)]
    (clojure.set/difference (set ids) (set matching-ids))))
