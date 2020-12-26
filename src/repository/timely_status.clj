(ns repository.timely-status
  (:require [korma.core :as db])
  (:use [korma.db]
        [repository.database-schema]
        [repository.query-executor]
        [utils.string]))

(declare timely-status)

(defn get-timely-status-model
  [connection]
  (db/defentity timely-status
                (db/pk :id)
                (db/table :timely_status)
                (db/database connection)
                (db/entity-fields
                  :publication_date_time
                  :member_name
                  :aggregate_id
                  :aggregate_name
                  :status_id
                  :time_range))
  timely-status)

(defn get-timely-statuses-for-aggregate
  "Get total statuses and ids of statuses related to the aggregate,
  which name is passed as first argument for a given week of the year"
  ; Relies on raw statuses
  ([{aggregate-id             :aggregate-id
     aggregate-name           :aggregate-name
     publication-week         :publication-week
     publication-year         :publication-year
     are-archived             :are-archived
     exclude-member-aggregate :exclude-member-aggregate}]
   (let [table-name (if are-archived
                      "weaving_archived_status"
                      "weaving_status")
         join-table-name (if are-archived
                           "weaving_archived_status_aggregate"
                           "weaving_status_aggregate")
         join-condition (cond
                          (some? exclude-member-aggregate)
                          "AND a.screen_name IS NULL "
                          (some? are-archived)
                          ""
                          :else
                          "AND a.screen_name IS NOT NULL ")
         restriction-by-aggregate-id (if aggregate-id
                                       "AND a.id = ? "
                                       "")
         restriction-by-week (if publication-week
                               "AND EXTRACT(WEEK FROM s.ust_created_at) = ?"
                               "")
         query (str
                 "SELECT                                           "
                 "COUNT(*) \"total-timely-statuses\",              "
                 "COALESCE(                                        "
                 "  array_to_string(array_agg(s.ust_id), ','),     "
                 "  ''                                             "
                 ") \"statuses-ids\"                               "
                 "FROM " table-name " s                            "
                 "INNER JOIN " join-table-name " sa                "
                 "ON sa.status_id = s.ust_id                       "
                 "INNER JOIN publishers_list a                   "
                 "ON (a.id = sa.aggregate_id                       "
                 "AND a.screen_name = s.ust_full_name              "
                 "AND a.name = ?)                                  "
                 join-condition
                 "AND (s.ust_id, a.id) NOT IN (                    "
                 "  SELECT status_id, aggregate_id                 "
                 "  FROM timely_status                             "
                 ")                                                "
                 "AND EXTRACT(YEAR FROM s.ust_created_at) = ?                   "
                 restriction-by-aggregate-id
                 restriction-by-week)
         params [aggregate-name publication-year]
         may-include-aggregate-id (if aggregate-id
                                    (conj params aggregate-id)
                                    params)
         may-include-week (if publication-week
                            (conj params publication-week)
                            may-include-aggregate-id)
         results (db/exec-raw [query may-include-week] :results)
         record (first results)
         ids (explode #"," (:statuses-ids record))
         total-timely-statuses (:total-timely-statuses record)
         statuses-ids (if (= (first ids) "") '(0) (map #(Long/parseLong %) ids))]
     {:statuses-ids          statuses-ids
      :total-timely-statuses total-timely-statuses}))
  ([aggregate-name publication-week publication-year & [are-archived]]
   (get-timely-statuses-for-aggregate {:aggregate-name   aggregate-name
                                       :publication-week publication-week
                                       :publication-year publication-year
                                       :are-archived     are-archived})))

(defn find-timely-statuses-by-aggregate-and-publication-date
  "Find the timely status statuses of a member published on a given day"
  ; Relies on available timely statuses
  ([aggregate-name]
   (let [results (find-timely-statuses-by-aggregate-and-publication-date aggregate-name nil)]
     results))
  ([aggregate-name publication-date]
   (let [base-query (str "
                      SELECT
                      ts.status_id as \"status-id\"
                      FROM timely_status ts
                      WHERE ts.aggregate_name = ?
                    ")
         query (if (nil? publication-date)
                 (str base-query "AND NOW()::date <= ts.publication_date_time::date ")
                 (str base-query "AND CAST(? AS date) = ts.publication_date_time::date "))
         params (if (nil? publication-date)
                  [aggregate-name]
                  [aggregate-name publication-date])
         query (str query "GROUP BY ts.status_id")
         results (db/exec-raw [query params] :results)]
     results)))

(defn find-aggregate-having-publication-from-date
  "Find the distinct aggregates for which publications have been collected for a given date"
  [date aggregate & [include-aggregate]]
  (let [aggregate-clause (str (if (some? include-aggregate)
                                "   AND aggregate_name = (?) "
                                "   AND aggregate_name != (?) "))
        query (str
                "SELECT DISTINCT aggregate_name as \"aggregate-name\"   "
                "FROM (                                               "
                "   SELECT aggregate_name FROM timely_status          "
                "   WHERE publication_date_time::date = CAST(? as date)  "
                aggregate-clause
                "   GROUP BY aggregate_id, aggregate_name             "
                ") select_")
        query (str query)
        results (db/exec-raw [query [date aggregate]] :results)]
    results))

(defn select-fields
  [model status-model member-model]
  (let [status-api-document-col (get-column "ust_api_document" status-model)
        status-id-col (get-column "ust_id" status-model)
        member-id-col (get-column "usr_id" member-model)
        member-name-col (get-column "usr_twitter_username" member-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [status-api-document-col :status-api-document]
                 [member-id-col :member-id]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name]
                 [:member_name :member-name]
                 [:status_id :status-id]
                 [:publication_date_time :publication-date-time])
      (db/join status-model (= status-id-col :status_id))
      (db/join member-model (= member-name-col :member_name)))))

(defn find-by-ids
  "Find timely statuses by their ids"
  [timely-statuses-ids {model        :timely-status
                        status-model :status
                        member-model :members}]
  (let [ids (if timely-statuses-ids timely-statuses-ids '(0))
        matching-statuses (-> (select-fields model status-model member-model)
                              (db/where {:id [in ids]})
                              (db/select))]
    (if matching-statuses
      matching-statuses
      '())))

(defn find-last-week-timely-status
  []
  (let [query (str
                "SELECT
                ts.id,
                s.ust_api_document as \"status-api-document\",
                e.member_id as \"member-id\",
                ts.member_name as \"member-name\",
                ts.aggregate_name as \"aggregate-name\",
                ts.status_id as \"status-id\",
                ts.publication_date_time as \"publication-date-time\"
                FROM publication_batch_collected_event e
                INNER JOIN weaving_user m
                ON m.usr_id = e.member_id
                AND DATEDIFF(NOW(), e.occurred_at) <= 7
                INNER JOIN timely_status ts
                ON member_name = m.usr_twitter_username
                LEFT JOIN keyword k
                ON k.status_id = ts.status_id
                INNER JOIN weaving_status s
                ON s.ust_id = ts.status_id                          
                WHERE k.id IS NULL
                LIMIT 10000;")
        query (str query)
        results (db/exec-raw [query []] :results)]
    results))

(defn find-timely-statuses-by-constraints
  "Find timely statuses by ids of statuses or constraints"
  ([constraints {model        :timely-status
                 member-model :members
                 status-model :status}]
   (let [{columns        :columns
          values         :values
          default-values :default-values} constraints
         constraining-values (if (pos? (count values)) values default-values)
         matching-statuses (-> (select-fields model status-model member-model)
                               (db/where (in columns constraining-values))
                               (db/select))]
     (if matching-statuses
       matching-statuses
       '())))
  ([statuses-ids aggregate-name {model        :timely-status
                                 member-model :members
                                 status-model :status}]
   (let [ids (if statuses-ids statuses-ids '(0))
         matching-statuses (-> (select-fields model status-model member-model)
                               (db/where (and
                                           (= :aggregate_name aggregate-name)
                                           (in :status_id ids)))
                               (db/select))]
     (if matching-statuses
       matching-statuses
       '()))))

(defn find-timely-statuses-by-aggregate-name
  [aggregate-name models]
  (find-timely-statuses-by-constraints
    {:columns        [:aggregate_name]
     :default-values '("")
     :values         [aggregate-name]} models))

(defn find-timely-statuses-by-aggregate-id
  [aggregate-id models]
  (find-timely-statuses-by-constraints
    {:columns        [:aggregate_id]
     :default-values '(0)
     :values         [aggregate-id]} models))

(defn bulk-insert-timely-statuses-from-aggregate
  [aggregate-id]
  (let [query (str "
                   INSERT INTO timely_status (
                     id,
                     status_id,
                     member_name,
                     aggregate_id,
                     aggregate_name,
                     publication_date_time,
                     time_range
                   )
                   SELECT
                   UUID() AS id,
                   s.ust_id AS status_id,
                   s.ust_full_name AS member_name,
                   a.id AS aggregate_id,
                   a.name AS aggregate_name,
                   s.ust_created_at AS publication_date_time,
                   CASE
                   WHEN s.ust_created_at > NOW()::timestamp - '5 MINUTES'::INTERVAL THEN 0
                   WHEN s.ust_created_at > NOW()::timestamp - '10 MINUTES'::INTERVAL THEN 1
                   WHEN s.ust_created_at > NOW()::timestamp - '30 MINUTES'::INTERVAL THEN 2
                   WHEN s.ust_created_at > NOW()::timestamp - '1 DAY'::INTERVAL THEN 3
                   WHEN s.ust_created_at > NOW()::timestamp - '1 WEEK'::INTERVAL THEN 4
                   ELSE 5
                   END AS time_range
                   FROM publishers_list AS a
                   INNER JOIN weaving_status_aggregate sa
                   ON sa.aggregate_id = a.id
                   INNER JOIN weaving_status AS s
                   ON s.ust_id = sa.status_id
                   WHERE a.id = ?
                   AND (s.ust_id, a.id) NOT IN (
                    SELECT status_id, aggregate_id FROM timely_status
                   )")
        count (exec-query [query [aggregate-id]])]
    (first count)))