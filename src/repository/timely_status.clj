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
  ([aggregate-name publication-week publication-year & [are-archived]]
   (let [table-name (if are-archived
                      "weaving_archived_status"
                      "weaving_status")
         join-table-name (if are-archived
                            "weaving_archived_status_aggregate"
                            "weaving_status_aggregate")
         join-condition (if are-archived
                          ""
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
      record (first results)]
     (if record
       {:statuses-ids (map #(Long/parseLong %) (explode #"," (:statuses-ids record)))
        :total-timely-statuses (:total-timely-statuses record)}
       {:statuses-ids '()
        :total-timely-statuses 0}))))

(defn find-timely-statuses-props-for-aggregate
  "Find the statuses of a member published on a given day"
  ; Relies on original statuses
  ([ids]
   (let [bindings (take (count ids) (iterate (constantly "?") "?"))
         more-bindings (string/join "," bindings)
         query (str
                  "SELECT                                           "
                  "a.id as `aggregate-id`,                          "
                  "a.name as `aggregate-name`,                      "
                  "s.ust_id as `status-id`,                         "
                  "s.ust_full_name as `member-name`,                "
                  "s.ust_created_at as `publication-date-time`      "
                  "FROM weaving_status s                            "
                  "INNER JOIN weaving_status_aggregate sa           "
                  "ON sa.status_id = s.ust_id                       "
                  "INNER JOIN weaving_aggregate a                   "
                  "ON a.id = sa.aggregate_id                       ")
         query (str query "AND s.ust_id IN (" more-bindings ")")
      results (db/exec-raw [query ids] :results)]
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
 [statuses-ids model status-model]
 (let [ids (if statuses-ids statuses-ids '(0))
       matching-statuses (-> (select-fields model status-model)
                             (db/where {:status_id [in ids]})
                             (db/select))]
   (if matching-statuses
     matching-statuses
     '())))

(defn bulk-insert
 [timely-statuses model status-model]
 (let [snake-cased-values (map snake-case-keys timely-statuses)
       identified-props (map #(assoc % :id (uuid/to-string (uuid/v1))) snake-cased-values)
       ids (map #(:id %) identified-props)]
   (if (pos? (count ids))
     (do
       (try
         (db/insert model (db/values identified-props))
         (catch Exception e (log/error (.getMessage e))))
       (find-by-ids ids model status-model))
     '())))