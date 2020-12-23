(ns repository.keyword
  (:require [clj-uuid :as uuid]
            [korma.core :as db])
  (:use [korma.db]
        [utils.string]
        [repository.database-schema]
        [repository.status-aggregate]
        [repository.query-executor]
        [twitter.status-hash]))

(declare hashtag)

(defn get-keyword-model
  [connection]
  (db/defentity hashtag
                (db/table :keyword)
                (db/database connection)
                (db/entity-fields
                  :id
                  :aggregate_id
                  :aggregate_name
                  :publication_date_time
                  :member_id
                  :status_id
                  :occurrences
                  :keyword))
  hashtag)

(defn get-keywords-props
  []
  [:keyword :occurrences :member-id :aggregate-id :aggregate-name :status-id])

(defn select-keywords
  [model member-model status-model]
  (let [status-api-document-col (get-column "ust_api_document" status-model)
        member-url-col (get-column "url" member-model)
        member-description-col (get-column "description" member-model)
        status-id-col (get-column "ust_id" status-model)
        status-twitter-id-col (get-column "ust_status_id" status-model)
        screen-name-col (get-column "ust_full_name" status-model)
        status-text-col (get-column "ust_text" status-model)
        member-id-col (get-column "usr_id" member-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [status-api-document-col :status-api-document]
                 [member-url-col :member-url]
                 [status-text-col :status]
                 [status-text-col :text]
                 [status-twitter-id-col :status-twitter-id]
                 [screen-name-col :screen-name]
                 [member-description-col :member-description]
                 [:occurrences :occurrences]
                 [:keyword :keyword]
                 [:member_id :member-id]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name]
                 [:status_id :status-id]
                 [:publication_date_time :created-at]
                 [:publication_date_time :publication-date-time])
      (db/join member-model (= member-id-col :member_id))
      (db/join status-model (= status-id-col :status_id)))))

(defn find-keywords-having-ids
  "Find keywords by their uuids"
  [keywords-ids model member-model status-model]
  (let [ids (if keywords-ids keywords-ids '(0))
        matching-keywords (-> (select-keywords model member-model status-model)
                              (db/where {:id [in ids]})
                              (db/select))]
    (if matching-keywords
      matching-keywords
      '())))

(defn find-statuses-containing-keyword
  "Find statuses by keyword"
  [keyword {model :keyword
            member-model :member
            status-model :status}]
  (let [matching-keywords (-> (select-keywords model member-model status-model)
                              (db/where {:keyword keyword})
                              (db/order :publication_date_time "DESC")
                              (db/select))]
    (if matching-keywords
      matching-keywords
      '())))

(defn find-keywords-by-aggregate-name
  "Find keywords by aggregate-name"
  [aggregate-name & args]
  (let [limit (if (some? args)
                (first args)
                100)
        query (str "
          SELECT SUM(occurrences) as `occurrences`,
          aggregate_id as `aggregate-id`,
          aggregate_name as `aggregate-name`,
          keyword as `keyword`
          FROM keyword
          WHERE aggregate_name = ?
          AND LENGTH(keyword) > 3
          GROUP BY keyword
          ORDER BY SUM(occurrences) DESC
          LIMIT ?
        ")
        results (db/exec-raw [query [aggregate-name limit]] :results)]
    (if results
      results
      '())))

(defn find-mentions-by-aggregate-name
  "Find mentions by aggregate-name"
  [aggregate-name & args]
  (let [limit (if (some? args)
                (first args)
                100)
        query (str "
          SELECT SUM(occurrences) as `occurrences`,
          aggregate_id as `aggregate-id`,
          aggregate_name as `aggregate-name`,
          keyword as `keyword`
          FROM keyword
          WHERE aggregate_name = ?
          AND keyword LIKE \"@%\"
          GROUP BY keyword
          ORDER BY SUM(occurrences) DESC
          LIMIT ?
        ")
        results (db/exec-raw [query [aggregate-name limit]] :results)]
    (if results
      results
      '())))

(defn find-keywords-for-statuses-ids
  "Find keywords by their uuids"
  [statuses-ids model member-model status-model]
  (let [ids (if statuses-ids statuses-ids '(0))
        matching-keywords (-> (select-keywords model member-model status-model)
                              (db/modifier "DISTINCT")
                              (db/where {:status_id [in ids]})
                              (db/select))]
    (if matching-keywords
      matching-keywords
      '())))

(defn bulk-insert-new-keywords
  [keywords {member-model :members
             model        :hashtag
             status-model :status} & [find-keywords]]
  (let [snake-cased-values (map snake-case-keys keywords)
        identified-props (pmap
                           #(assoc % :id (-> (uuid/v1) (uuid/v5 (:aggregate_name %))))
                           snake-cased-values)
        ids (map #(:id %) identified-props)]
    (if (pos? (count ids))
      (bulk-insert-and-find-on-condition
        identified-props
        model
        (when (some? find-keywords)
          #((find-keywords-having-ids ids model member-model status-model))))
      '())))