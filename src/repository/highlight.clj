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

(defn find-today-statuses-for-aggregate
  "Find a single status for each member"
  [aggregate-name]
  (let [query (str
                "SELECT s.ust_id as `status-id`,                           "
                "m.usr_id as `member-id`,                                  "
                "s.ust_api_document as `api-document`,                     "
                "s.ust_created_at as `publication-date-time`               "
                "FROM weaving_status s                                     "
                "INNER JOIN weaving_status_aggregate sa                    "
                "ON s.ust_id = sa.status_id                                "
                "INNER JOIN weaving_aggregate a                            "
                "ON a.name = ?                                             "
                "AND a.id = sa.aggregate_id                                "
                "INNER JOIN weaving_user m                                  "
                "ON m.usr_twitter_username = s.ust_full_name               "
                "WHERE DATE(now()) <= s.ust_created_at                     ")
        results (db/exec-raw [query [aggregate-name]] :results)]
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
                [:status_id :status-id]
                [:publication_date_time :publication-date-time])
     (db/join member-model (= member-id-col :member_id))
     (db/join status-model (= status-id-col :status_id)))))

(defn find-highlights-having-ids
 "Find highlights by their ids"
 [ids model member-model status-model]
 (let [ids (if ids ids '(0))
       matching-statuses (-> (select-highlights model member-model status-model)
                             (db/where {:ust_status_id [in ids]})
                             (db/group :ust_status_id)
                             (db/select))]
   (if matching-statuses
     matching-statuses
     '())))

(defn bulk-insert-new-highlights
 [highlights model member-model status-model]
 (let [snake-cased-values (map snake-case-keys highlights)
       ids (map #(:id %) snake-cased-values)]
   (if (pos? (count ids))
     (do
       (try
         (db/insert model (db/values snake-cased-values))
         (catch Exception e (log/error (.getMessage e))))
       (find-highlights-having-ids ids model member-model status-model))
     '())))