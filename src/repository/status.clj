(ns repository.status
  (:require [korma.core :as db]
            [clojure.string :as string]
            [clojure.data.json :as json]
            [utils.error-handler :as error-handler])
  (:use [repository.database-schema]
        [korma.db]
        [utils.string]
        [twitter.status-hash]))

(declare status)

(defn get-status-model
  [connection]
  (db/defentity status
                (db/pk :ust_id)
                (db/table :weaving_status)
                (db/database connection)
                (db/entity-fields
                  :ust_id
                  :ust_hash                                 ; sha1(str ust_text  ust_status_id)
                  :ust_text
                  :ust_full_name                            ; twitter user screen name
                  :ust_name                                 ; twitter user full name
                  :ust_access_token
                  :ust_api_document
                  :is_published
                  :ust_created_at
                  :ust_status_id))
  status)

(defn select-statuses
  [model]
  (->
    (db/select* model)
    (db/fields [:ust_id :id]
               [:ust_id :status-id]
               [:ust_hash :hash]
               [:ust_avatar :avatar-url]
               [:ust_text :text]
               [:ust_full_name :screen-name]
               [:ust_name :name]
               [:ust_access_token :access-token]
               [:ust_api_document :document]
               [:ust_created_at :created-at]
               [:ust_status_id :status-twitter-id]
               [:ust_status_id :twitter-id]
               [:is_published :is-published])))

(defn find-statuses-having-column-matching-values
  "Find statuses which values of a given column
  can be found in collection passed as argument"
  [column values model]
  (let [values (if values values '(0))
        matching-statuses (-> (select-statuses model)
                              (db/where {column [in values]})
                              (db/group :ust_status_id
                                        :ust_id
                                        :ust_hash
                                        :ust_avatar
                                        :ust_text
                                        :ust_full_name
                                        :ust_name
                                        :ust_access_token
                                        :ust_api_document
                                        :ust_created_at
                                        :ust_status_id
                                        :ust_status_id
                                        :is_published)
                              (db/order :ust_created_at "DESC")
                              (db/select))]
    (if matching-statuses
      matching-statuses
      '())))

(defn select-identifying-fields
  [model]
  (->
    (db/select* model)
    (db/fields [:ust_id :status-id]
               [:ust_status_id :status-twitter-id])))

(defn find-by-hashes
  "Find statuses by hashes"
  [hashes model]
  (let [hashes (if hashes hashes '(0))
        matching-statuses (-> (select-identifying-fields model)
                              (db/where {:ust_hash [in hashes]})
                              (db/select))]
    (if matching-statuses
      matching-statuses
      '())))

(defn find-statuses-having-twitter-ids
  "Find statuses by their Twitter ids"
  [twitter-ids model]
  (find-statuses-having-column-matching-values :ust_status_id twitter-ids model))

(defn find-statuses-having-ids
  "Find statuses by their ids"
  [ids model]
  (find-statuses-having-column-matching-values :ust_id ids model))

(defn find-statuses-by-screen-name
  "Find statuses by a screen name"
  [screen-name model]
  (find-statuses-having-column-matching-values :ust_full_name (apply list [screen-name]) model))

(defn find-statuses-for-aggregate-authored-by
  "Find statuses by their author for a given aggregate"
  [authors aggregate-id]
  (let [bindings (take (count authors) (iterate (constantly "?") "?"))
        more-bindings (string/join "," bindings)
        query (str "
          SELECT ust_id AS id,
          ust_hash AS hash,
          ust_text AS text,
          ust_full_name AS \"screen-name\",
          ust_name AS \"name\",
          ust_access_token AS \"access-token\",
          ust_api_document AS \"document\",
          ust_created_at AS \"created-at\",
          ust_status_id AS \"twitter-id\"
          FROM weaving_status
          WHERE ust_full_name in (" more-bindings ")
          AND (ust_full_name, ust_id) NOT IN (
             SELECT member_name, status_id
             FROM timely_status
             WHERE aggregate_id = ?
          )
        ")
        params (conj authors aggregate-id)
        results (db/exec-raw [query params] :results)]
    (if (some? results)
      results
      '())))

(defn find-statuses-by-week-and-author
  [week year screen-name]
  (let [query (str
                "SELECT ust_id AS id,                       "
                "ust_hash AS hash,                          "
                "ust_text AS text,                          "
                "ust_full_name AS \"screen-name\",            "
                "ust_name AS \"name\",                        "
                "ust_access_token AS \"access-token\",        "
                "ust_api_document AS \"document\",            "
                "ust_created_at AS \"created-at\",            "
                "ust_status_id AS \"twitter-id\"              "
                "FROM weaving_status                        "
                "WHERE ust_full_name = ?                    "
                "AND EXTRACT(WEEK FROM ust_created_at) = ?               "
                "AND EXTRACT(YEAR FROM ust_created_at) = ?               ")
        params [screen-name week year]
        results (db/exec-raw [query params] :results)]
    (if (some? results)
      results
      '())))

(defn insert-values-before-selecting-from-ids
  [values twitter-ids model]
  (if (pos? (count twitter-ids))
    (do
      (try
        (db/insert model (db/values values))
        (catch Exception e
          (error-handler/log-error e)))
      (find-statuses-having-twitter-ids twitter-ids model))
    '()))

(defn assoc-avatar
  [status]
  (let [raw-document (:ust_api_document status)
        decoded-document (json/read-str raw-document)
        user (get decoded-document "user")
        avatar (get user "profile_image_url_https")]
    (assoc status :ust_avatar avatar)))

(defn is-subset-of
  [statuses-set]
  (fn [status]
    (let [status-id (:twitter-id status)]
      (clojure.set/subset? #{status-id} statuses-set))))

(defn bulk-unarchive-statuses
  [statuses model]
  (let [statuses-twitter-ids (map #(:twitter-id %) statuses)
        existing-statuses (find-statuses-having-twitter-ids statuses-twitter-ids model)
        existing-statuses-twitter-ids (map #(:twitter-id %) existing-statuses)
        filtered-statuses (doall (remove (is-subset-of (set existing-statuses-twitter-ids)) statuses))
        statuses-props (map #(dissoc %
                                     :id
                                     :ust_id
                                     :screen-name
                                     :access-token
                                     :document
                                     :name
                                     :text
                                     :hash
                                     :created-at
                                     :twitter-id) filtered-statuses)
        statuses-props (map assoc-avatar statuses-props)
        deduped-statuses (dedupe (sort-by #(:status_id %) statuses-props))
        twitter-ids (map #(:ust_status_id %) deduped-statuses)
        new-statuses (insert-values-before-selecting-from-ids deduped-statuses twitter-ids model)]
    (if (pos? (count new-statuses))
      new-statuses
      existing-statuses)))

(defn bulk-insert-new-statuses
  [statuses model]
  (let [is-subset-of (fn [statuses-set]
                       (fn [status]
                         (let [status-id (:status_id status)]
                           (clojure.set/subset? #{status-id} statuses-set))))
        snake-cased-values (map snake-case-keys statuses)
        statuses-props (map assoc-hash snake-cased-values)
        deduped-statuses (dedupe (sort-by #(:status_id %) statuses-props))
        statuses-hashes (map #(:hash %) deduped-statuses)
        existing-hashes (find-by-hashes statuses-hashes model)
        new-status-props (remove
                           (is-subset-of
                             (set (map #(:status-twitter-id %) existing-hashes)))
                           deduped-statuses)
        new-status-props (dedupe (sort-by #(:hash %) new-status-props))
        prefixed-keys-values (map prefixed-keys new-status-props)
        twitter-ids (map #(:ust_status_id %) prefixed-keys-values)]
    (insert-values-before-selecting-from-ids prefixed-keys-values twitter-ids model)))

(defn filter-duplicates-by-status
  []
  (fn [[_ status]]
    (> (count status) 1)))

(defn find-unpublished-statuses
  [model & [limit]]
  (let [limit (if (some? limit)
                limit
                50000)
        is-published-col (get-column "is_published" model)
        matching-statuses (-> (select-statuses model)
                              (db/where {is-published-col 0})
                              (db/limit limit)
                              (db/select))
        grouped-status (group-by :status-twitter-id matching-statuses)
        filtered-status (->> grouped-status
                            (remove (filter-duplicates-by-status))
                            vals
                           (map first))]
    (if (some? filtered-status)
      filtered-status
      '())))

(defn update-status-having-ids
  [status-ids model]
  (db/update
    model
    (db/set-fields {:is_published 1})
    (db/where {:ust_id [in status-ids]})))
