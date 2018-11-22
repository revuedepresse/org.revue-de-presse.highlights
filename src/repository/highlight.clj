(ns repository.highlight
  (:require [clojure.tools.logging :as log]
            [korma.core :as db]
            [clj-uuid :as uuid])
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
                  :publication_date_time
                  :member_id
                  :status_id
                  :total_favorites
                  :total_retweets))
  highlight)

(defn find-statuses-for-aggregate
  "Find a single status for each member"
  ([aggregate-name]
   (let [results (find-statuses-for-aggregate aggregate-name nil)]
    results))
  ([aggregate-name publication-date]
   (let [base-query (str
                      "SELECT s.ust_id as `status-id`,              "
                      "m.usr_id as `member-id`,                     "
                      "s.ust_api_document as `api-document`,        "
                      "s.ust_created_at as `publication-date-time`  "
                      "FROM timely_status ts                        "
                      "INNER JOIN weaving_status s                  "
                      "ON s.ust_id = ts.status_id                   "
                      "INNER JOIN weaving_user m                    "
                      "ON ts.member_name = m.usr_twitter_username   "
                      "WHERE ts.aggregate_name = ?                  ")
        query (if (nil? publication-date)
                (str base-query "AND DATE(now()) <= ts.publication_date_time")
                (str base-query "AND ? = DATE(ts.publication_date_time)"))
       params (if (nil? publication-date)
                  [aggregate-name]
                  [aggregate-name publication-date])
      results (db/exec-raw [query params] :results)]
    results)))

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
                [:status_id :status-id]
                [:publication_date_time :publication-date-time])
     (db/join member-model (= member-id-col :member_id))
     (db/join status-model (= status-id-col :status_id)))))

(defn find-highlights-having-ids
 "Find highlights by their ids"
 [highlights-ids model member-model status-model]
 (let [ids (if highlights-ids highlights-ids '(0))
       matching-statuses (-> (select-highlights model member-model status-model)
                             (db/where {:status_id [in ids]})
                             (db/select))]
   (if matching-statuses
     matching-statuses
     '())))

(defn bulk-insert-new-highlights
 [highlights model member-model status-model]
 (let [snake-cased-values (map snake-case-keys highlights)
       ids (map #(:status_id %) snake-cased-values)]
   (if (pos? (count ids))
     (do
       (try
         (db/insert model (db/values snake-cased-values))
         (catch Exception e (log/error (.getMessage e))))
       (find-highlights-having-ids ids model member-model status-model))
     '())))