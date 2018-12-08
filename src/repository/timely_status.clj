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
  ([{aggregate-name :aggregate-name
     publication-week :publication-week
     publication-year :publication-year
     are-archived :are-archived
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
                 join-condition
                 "AND a.name = ?)                                  "
                 "AND WEEK(s.ust_created_at) = ?                   "
                 "AND YEAR(s.ust_created_at) = ?")
         params [aggregate-name publication-week publication-year]
         results (db/exec-raw [query params] :results)
         record (first results)
         ids (explode #"," (:statuses-ids record))
         total-timely-statuses (:total-timely-statuses record)
         statuses-ids (if (= (first ids) "") '(0) (map #(Long/parseLong %) ids))]
     {:statuses-ids statuses-ids
      :total-timely-statuses total-timely-statuses}))
  ([aggregate-name publication-week publication-year & [are-archived]]
   (get-timely-statuses-for-aggregate {:aggregate-name aggregate-name
                                       :publication-week publication-week
                                       :publication-year publication-year
                                       :are-archived are-archived})))

(defn find-timely-statuses-props-for-aggregate
  "Find the statuses of a member published on a given day"
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
                  "ON a.id = sa.aggregate_id                                ")
         query (str query "AND s.ust_id IN (" more-bindings ")")
      results (db/exec-raw [query params] :results)]
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
 ([statuses-ids model status-model]
 (let [ids (if statuses-ids statuses-ids '(0))
       matching-statuses (-> (select-fields model status-model)
                             (db/where {:status_id [in ids]})
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

(defn bulk-insert
 [timely-statuses aggregate-name model status-model]
 (let [snake-cased-values (pmap snake-case-keys timely-statuses)
       identified-props (pmap
                          #(assoc % :id (uuid/to-string
                                          (-> (uuid/v1) (uuid/v5 aggregate-name))))
                          snake-cased-values)
       statuses-ids (pmap #(:status_id %) identified-props)
       existing-timely-statuses (find-by-statuses-ids statuses-ids aggregate-name model status-model)
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