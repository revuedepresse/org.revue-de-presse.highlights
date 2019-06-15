(ns repository.keyword
  (:require [clojure.tools.logging :as log]
            [clj-uuid :as uuid]
            [korma.core :as db])
  (:use [korma.db]
        [utils.string]
        [repository.database-schema]
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

(defn select-keywords
  [model member-model status-model]
  (let [status-api-document-col (get-column "ust_api_document" status-model)
        member-url-col (get-column "url" member-model)
        member-description-col (get-column "description" member-model)
        status-id-col (get-column "ust_id" status-model)
        status-text-col (get-column "ust_text" status-model)
        member-id-col (get-column "usr_id" member-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [status-api-document-col :status-api-document]
                 [member-url-col :member-url]
                 [status-text-col :status]
                 [member-description-col :member-description]
                 [:occurrences :occurrences]
                 [:keyword :keyword]
                 [:member_id :member-id]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name]
                 [:status_id :status-id]
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
  [keywords
   {member-model :members
    model        :hashtag
    status-model :status}
   & [find-keywords]]
  (let [snake-cased-values (map snake-case-keys keywords)
        identified-props (pmap
                           #(assoc % :id (uuid/to-string
                                           (-> (uuid/v1) (uuid/v5 (:aggregate_name %)))))
                           snake-cased-values)
        ids (map #(:id %) identified-props)]
    (if (pos? (count ids))
      (bulk-insert-and-find-on-condition
        identified-props
        model
        (when (some? find-keywords)
          #((find-keywords-having-ids ids model member-model status-model))))
      '())))