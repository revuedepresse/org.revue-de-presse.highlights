(ns repository.timely-status
  (:require [clojure.tools.logging :as log]
            [korma.core :as db])
  (:use [korma.db]
        [repository.database-schema]
        [utils.string]))

(declare timely_status)

(defn get-timely-status-model
  [connection]
  (get-model {:connection connection
              :model timely_status
              :table :timely_status
              :fields [:publication_date_time
                       :member_id
                       :status_id
                       :total_favorites
                       :total_retweets]}))

(defn find-raw-statuses-for-aggregate
  "Find the statuses of a member published on a given day"
  ; Relies on original statuses
  ([aggregate-name]
   (let [results (find-raw-statuses-for-aggregate aggregate-name nil)]
    results))
  ([aggregate-name publication-date]
   (let [base-query (str
                      "SELECT                                           "
                      "s.ust_id as `status-id`,                         "
                      "m.usr_id as `member-id`,                         "
                      "s.ust_api_document as `api-document`,            "
                      "s.ust_created_at as `publication-date-time`      "
                      "FROM weaving_status s                            "
                      "INNER JOIN weaving_status_aggregate sa            "
                      "ON sa.status_id = s.ust_id                       "
                      "INNER JOIN weaving_aggregate a                    "
                      "ON a.id = sa.aggregate_id                        "
                      "AND a.screen_name = ?                            "
                      "INNER JOIN weaving_user m                        "
                      "ON s.ust_screen_name = m.usr_twitter_username    "
                      "WHERE s.name = ?")
        query (if (nil? publication-date)
                (str base-query "AND DATE(now()) <= s.ust_created_at")
                (str base-query "AND ? = DATE(ts.ust_created_at\")"))
       params (if (nil? publication-date)
                  [aggregate-name aggregate-name]
                  [aggregate-name aggregate-name publication-date])
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
                 [:member_id :member-id]
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

(defn bulk-insert
 [timely-statuses model status-model]
 (let [snake-cased-values (map snake-case-keys timely-statuses)
       ids (map #(:id %) snake-cased-values)]
   (if (pos? (count ids))
     (do
       (try
         (db/insert model (db/values snake-cased-values))
         (catch Exception e (log/error (.getMessage e))))
       (find-by-ids ids model status-model))
     '())))