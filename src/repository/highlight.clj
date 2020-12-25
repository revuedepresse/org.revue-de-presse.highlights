(ns repository.highlight
  (:require [korma.core :as db]
            [clojure.string :as string]
            [utils.error-handler :as error-handler])
  (:use [korma.db]
        [repository.database-schema]
        [utils.string]))

(declare highlight)

(defn get-highlight-model
  [connection]
  (db/defentity highlight
                (db/pk :id)
                (db/table :highlight)
                (db/database connection)
                (db/entity-fields
                  :aggregate_id
                  :aggregate_name
                  :is_retweet
                  :member_id
                  :publication_date_time
                  :retweeted_status_publication_date
                  :status_id
                  :total_favorites
                  :total_retweets))
  highlight)

(defn find-statuses-by-ids
  "Find the statuses of a member published on a given day"
  ([ids]
   (let [bindings (take (count ids) (iterate (constantly "?") "?"))
         bindings (string/join "," bindings)
         base-query (str "
                      SELECT
                      s.ust_id as \"status-id\",
                      m.usr_id as \"member-id\",
                      s.ust_api_document as \"api-document\",
                      s.ust_created_at as \"publication-date-time\"
                      FROM timely_status ts
                      INNER JOIN weaving_status s
                      ON s.ust_id = ts.status_id
                      LEFT JOIN weaving_user m
                      ON ts.member_name = m.usr_twitter_username
                      WHERE ts.status_id IN (" bindings ")
                      GROUP BY
                      ts.status_id,
                      s.ust_id,
                      m.usr_id,
                      s.ust_api_document,
                      s.ust_created_at
                    ")
         ; there could be multiple timely statuses
         ; having similar statuses ids
         results (db/exec-raw [base-query ids] :results)]
     results)))

(defn find-highlights-for-aggregate-published-at
  "Find highlights published on a given date"
  [date aggregate-name]
  (let [query (str
                "SELECT                                           "
                "s.ust_id as id,                                  "
                "s.ust_status_id as \"status-id\"                   "
                "FROM highlight h                                 "
                "INNER JOIN timely_status t                       "
                "ON t.status_id = h.status_id                     "
                "AND t.aggregate_name = ?                         "
                "INNER JOIN weaving_status s                      "
                "ON s.ust_id = h.status_id                        "
                "AND h.publication_date_time::date = CAST('" date "' AS DATE)")
        results (db/exec-raw [query [aggregate-name]] :results)]
    results))

(defn find-highlights-since-a-month-ago
  []
  (let [query (str "
                SELECT *
                FROM (
                   SELECT
                       h.id,
                       h.aggregate_id AS \"aggregate-id\",
                       h.aggregate_name AS \"aggregate-name\",
                       s.ust_full_name AS \"screen-name\",
                       CONCAT(
                               'https://twitter.com/',
                               m.screen_name, '/status/',
                               s.ust_status_id
                           ) AS \"status-url\",
                       s.ust_text AS \"text\",
                       h.total_retweets AS \"retweets\",
                       s.ust_created_at AS \"created-at\"
                   FROM highlight h
                   INNER JOIN weaving_status s
                   ON s.ust_id = h.status_id
                   AND h.aggregate_name not like 'user ::%'
                   AND h.total_retweets > 0
                   AND h.publication_date_time::date > NOW()::date - '1 MONTH'::INTERVAL)
                   INNER JOIN member_identity m
                   ON m.member_id = h.member_id
                   WHERE h.id IN (
                       SELECT highlights_by_aggregate.id FROM (
                           SELECT id, MAX(total_retweets), aggregate_name
                           FROM highlight
                           WHERE publication_date_time::date > NOW()::date - '1 MONTH'::INTERVAL)
                           GROUP BY aggregate_name
                      ) highlights_by_aggregate
                   )
                   ORDER BY h.aggregate_name ASC
                ) AS highlights
                ORDER BY highlights.`retweets`
              ")
        results (db/exec-raw [query] :results)]
    results))

(defn select-highlights
  [model member-model status-model]
  (let [status-api-document-col (get-column "ust_api_document" status-model)
        member-url-col (get-column "url" member-model)
        member-description-col (get-column "description" member-model)
        status-id-col (get-column "ust_id" status-model)
        member-id-col (get-column "usr_id" member-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [status-api-document-col :status-api-document]
                 [member-url-col :member-url]
                 [member-description-col :member-description]
                 [:member_id :member-id]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name]
                 [:is_retweet :is-retweet]
                 [:status_id :status-id]
                 [:retweeted_status_publication_date :retweeted-status-publication-date]
                 [:publication_date_time :publication-date-time])
      (db/join member-model (= member-id-col :member_id))
      (db/join status-model (= status-id-col :status_id)))))

(defn find-highlights-having-ids
  "Find highlights by their ids"
  [highlights-ids model member-model status-model]
  (let [ids (if highlights-ids highlights-ids '(0))
        matching-statuses (try
                            (-> (select-highlights model member-model status-model)
                                (db/where {:status_id [in ids]})
                                (db/select))
                            (catch Exception e
                              (error-handler/log-error e)))]
    (if matching-statuses
      matching-statuses
      '())))

(defn find-highlights-having-uuids
  "Find highlights by their uuids"
  [highlights-ids model member-model status-model]
  (let [ids (if highlights-ids highlights-ids '(0))
        matching-statuses (-> (select-highlights model member-model status-model)
                              (db/where {:id [in ids]})
                              (db/select))]
    (if matching-statuses
      matching-statuses
      '())))

(defn find-highlighted-statuses-for-aggregate-published-at
  "Find highlights published on a given date"
  [{date                      :date
    week                      :week
    year                      :year
    aggregate-name            :aggregate-name
    not-in                    :not-in
    {member-model    :members
     highlight-model :highlight
     status-model    :status} :models}]
  (let [aggregate-restriction (if not-in
                                "AND t.aggregate_name <> ? "
                                "AND t.aggregate_name = ? ")
        highlight-aggregate-restriction (if not-in
                                          "h.aggregate_name <> ? "
                                          "h.aggregate_name = ? ")
        restricted-by-week (and
                             (some? week)
                             (some? year))
        restriction-by-date (if restricted-by-week
                              (str
                                "AND EXTRACT(WEEK FROM h.publication_date_time) = ? "
                                "AND EXTRACT(YEAR FROM h.publication_date_time) = ? ")
                              (str "AND h.publication_date_time::date = CAST('" date "' AS DATE)"))
        query (str
                "SELECT                                           "
                "h.id                                             "
                "FROM highlight h                                 "
                "INNER JOIN timely_status t                       "
                "ON t.status_id = h.status_id                     "
                aggregate-restriction "                           "
                "INNER JOIN weaving_status s                      "
                "ON s.ust_id = h.status_id                        "
                "WHERE " highlight-aggregate-restriction "        "
                restriction-by-date)
        params (if restricted-by-week
                 [aggregate-name aggregate-name week year]
                 [aggregate-name aggregate-name])
        results (db/exec-raw [query params] :results)
        highlights-ids (map :id results)
        matching-highlights (find-highlights-having-uuids highlights-ids highlight-model member-model status-model)]
    matching-highlights))

(defn bulk-insert-new-highlights
  [highlights model member-model status-model]
  (let [snake-cased-values (->>
                             highlights
                             (map snake-case-keys)
                             (group-by #(:status_id %))
                             vals
                             (map first))
        ids (map #(:status_id %) snake-cased-values)]
    (if (pos? (count ids))
      (do
        (try
          (db/insert model (db/values snake-cased-values))
          (catch Exception e
            (cond
              :else (error-handler/log-error e))))
        (find-highlights-having-ids ids model member-model status-model))
      '())))