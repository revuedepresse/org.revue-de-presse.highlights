(ns repository.timely-status
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clj-uuid :as uuid]
            [korma.core :as db])
  (:use [korma.db]
        [repository.database-schema]
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
                               "AND WEEK(s.ust_created_at) = ?"
                               "")
         query (str
                 "SELECT                                           "
                 "COUNT(*) `total-timely-statuses`,                "
                 "IF (COUNT(*) > 0,                                "
                 "    GROUP_CONCAT(s.ust_id),                      "
                 "    \"\") `statuses-ids`                         "
                 "FROM " table-name " s                            "
                 "INNER JOIN " join-table-name " sa                "
                 "ON sa.status_id = s.ust_id                       "
                 "INNER JOIN weaving_aggregate a                   "
                 "ON (a.id = sa.aggregate_id                       "
                 "AND a.screen_name = s.ust_full_name              "
                 "AND a.name = ?)                                  "
                 join-condition
                 "AND (s.ust_id, a.id) NOT IN (                    "
                 "  SELECT status_id, aggregate_id                 "
                 "  FROM timely_status                             "
                 ")                                                "
                 "AND YEAR(s.ust_created_at) = ?                   "
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

(defn find-timely-statuses-props-for-aggregate
  "Find the timely statuses properties for an aggregate"
  ; Relies on original statuses
  ([ids]
   (let [bindings (take (count ids) (iterate (constantly "?") "?"))
         more-bindings (string/join "," bindings)
         params ids
         query (str
                 "SELECT DISTINCT                                          "
                 "a.id as `aggregate-id`,                                  "
                 "a.name as `aggregate-name`,                              "
                 "s.ust_id as `status-id`,                                 "
                 "s.ust_full_name as `member-name`,                        "
                 "s.ust_created_at as `publication-date-time`              "
                 "FROM weaving_status s                                    "
                 "INNER JOIN weaving_status_aggregate sa                   "
                 "ON sa.status_id = s.ust_id                               "
                 "INNER JOIN weaving_aggregate a                           "
                 "ON a.id = sa.aggregate_id                                "
                 "WHERE (a.id, s.ust_id) NOT IN (                          "
                 "  SELECT aggregate_id, status_id FROM timely_status      "
                 ")                                                        ")
         query (str query "AND s.ust_id IN (" more-bindings ")")
         results (db/exec-raw [query params] :results)]
     results)))

(defn find-missing-timely-statuses-from-aggregate
  "Find the statuses of a member published on a given day"
  ([aggregate-id]
   (let [query (str
                 "SELECT                                                            "
                 "sa.aggregate_id as `aggregate-id`,                                "
                 "a.name as `aggregate-name`,                                       "
                 "sa.status_id as `status-id`,                                      "
                 "s.ust_full_name as `member-name`,                                 "
                 "s.ust_created_at as `publication-date-time`                       "
                 "FROM weaving_status_aggregate sa                                  "
                 "INNER JOIN weaving_aggregate a                                    "
                 "ON (                                                              "
                 "  sa.aggregate_id = a.id                                          "
                 "  AND a.id = ?                                                    "
                 ")                                                                 "
                 "INNER JOIN weaving_status s                                       "
                 "ON (                                                              "
                 "    s.ust_id = sa.status_id                                       "
                 "    AND s.ust_full_name = a.screen_name                           "
                 ")                                                                 "
                 "WHERE (sa.status_id, sa.aggregate_id) NOT IN (                    "
                 "    SELECT COALESCE(status_id, 0), COALESCE(aggregate_id, 0)      "
                 "     FROM timely_status                                           "
                 ")                                                                 ")
         results (db/exec-raw [query [aggregate-id]] :results)]
     results)))

(defn find-aggregate-having-publication-from-date
  "Find the distinct aggregates for which publications have been collected for a given date"
  ([date excluded-aggregate]
   (let [query (str
                 "SELECT DISTINCT aggregate_name as `aggregate-name`   "
                 "FROM (                                               "
                 "   SELECT aggregate_name FROM timely_status          "
                 "   WHERE DATE(publication_date_time) = ?             "
                 "   AND aggregate_name != (?)                         "
                 "   GROUP BY aggregate_id                             "
                 ") select_")
         query (str query)
         results (db/exec-raw [query [date excluded-aggregate]] :results)]
     results)))

(defn select-fields
  [model status-model]
  (let [status-api-document-col (get-column "ust_api_document" status-model)
        status-id-col (get-column "ust_id" status-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [status-api-document-col :status-api-document]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name]
                 [:member_name :member-name]
                 [:status_id :status-id]
                 [:publication_date_time :publication-date-time])
      (db/join status-model (= status-id-col :status_id)))))

(defn find-by-ids
  "Find timely statuses by their ids"
  [timely-statuses-ids model status-model]
  (let [ids (if timely-statuses-ids timely-statuses-ids '(0))
        matching-statuses (-> (select-fields model status-model)
                              (db/where {:id [in ids]})
                              (db/select))]
    (if matching-statuses
      matching-statuses
      '())))

(defn find-by-statuses-ids
  "Find timely statuses by their ids"
  ([constraints model status-model]
   (let [{columns :columns
          values  :values
          default-values :default-values} constraints
         constraining-values (if (pos? (count values)) values default-values)
         matching-statuses (-> (select-fields model status-model)
                               (db/where (in columns constraining-values))
                               (db/select))]
     (if matching-statuses
       matching-statuses
       '())))
  ([statuses-ids aggregate-name model status-model]
   (let [ids (if statuses-ids statuses-ids '(0))
         matching-statuses (-> (select-fields model status-model)
                               (db/where (and
                                           (= :aggregate_name aggregate-name)
                                           (in :status_id ids)))
                               (db/select))]
     (if matching-statuses
       matching-statuses
       '()))))

(defn find-last-timely-status-by-aggregate
  [aggregate-id model status-model]
  (let [matching-statuses (-> (select-fields model status-model)
                              (db/where (= :aggregate_id aggregate-id))
                              (db/order :publication_date_time :DESC)
                              (db/limit 1)
                              (db/select))]
    (first matching-statuses)))

(defn bulk-insert
  [timely-statuses model status-model]
  (let [snake-cased-values (pmap snake-case-keys timely-statuses)
        identified-props (pmap
                           #(assoc % :id (uuid/to-string
                                           (-> (uuid/v1) (uuid/v5 (:aggregate_name %)))))
                           snake-cased-values)
        constraints (pmap #(apply list [(:status_id %) (:aggregate_name %)]) identified-props)
        existing-timely-statuses (find-by-statuses-ids
                                   {:columns [:status_id :aggregate_name]
                                    :default-values '((0 ""))
                                    :values constraints} model status-model)
        existing-statuses-id (pmap #(:status_id %) existing-timely-statuses)
        deduplicated-props (dedupe (sort-by #(:status_id %) identified-props))
        filtered-props (remove #(clojure.set/subset? #{(:status_id %)} existing-statuses-id) deduplicated-props)
        ids (pmap #(:id %) filtered-props)
        timely-statuses-to-be-inserted (pos? (count ids))]
    (if timely-statuses-to-be-inserted
      (do
        (try
          (db/insert model (db/values filtered-props))
          (catch Exception e (log/error (.getMessage e))))
        (find-by-ids ids model status-model))
      '())))