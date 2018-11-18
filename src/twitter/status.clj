(ns twitter.status
  (:require [clojure.data.json :as json]
            [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [repository.aggregate]
        [twitter.api-client]
        [twitter.date]
        [twitter.member]))

(def ^:dynamic *twitter-status-enabled-logging* false)

(defn get-ids-of-statuses
  [statuses]
  (map (fn [{twitter-id :id_str}] twitter-id) statuses))

(defn get-missing-statuses-ids
  [statuses model]
  (let [ids (get-ids-of-statuses statuses)
        found-statuses (find-statuses-having-ids ids model)
        matching-ids (set (map #(:twitter-id %) found-statuses))
        missing-ids (clojure.set/difference (set ids) (set matching-ids))]
    missing-ids))

(defn get-status-json
      [{twitter-id :id_str
        text :full_text
        created-at :created_at
        {screen-name :screen_name
         avatar :profile_image_url
         name :name} :user
        :as status}]
      (let [document (json/write-str status)
            token (:token @next-token)
            parsed-publication-date (c/to-long (f/parse date-formatter created-at))
            mysql-formatted-publication-date (f/unparse mysql-date-formatter (c/from-long parsed-publication-date))
            twitter-status {:text text
                            :full-name screen-name
                            :avatar avatar
                            :name name
                            :access-token token
                            :api-document document
                            :created-at mysql-formatted-publication-date
                            :status-id twitter-id}]
        twitter-status))

(defn ensure-statuses-exist
  [statuses model]
  (let [total-statuses (count statuses)
        new-statuses (if
                       (pos? total-statuses)
                       (do
                         (log/info (str "About to ensure " total-statuses " statuses exist."))
                         (bulk-insert-new-statuses (pmap #(get-status-json %) statuses) model))
                       (do
                         (log/info (str "No need to find some missing status."))
                         '()))]
    (when (and
            *twitter-status-enabled-logging*
           (pos? total-statuses))
      (doall (map #(log/info (str "Status #" (:twitter-id %)
                                  " authored by \"" (:screen-name %)
                                  "\" has been saved under id \"" (:id %))) new-statuses)))
    new-statuses))

(defn get-id-as-string
  [status]
  (let [id-as-string (:id_str status)
        id-set #{id-as-string}]
    id-set))

(defn in-ids-as-string-set
  [set]
  (fn [status]
    (let [id-singleton (get-id-as-string status)]
      (clojure.set/subset? id-singleton set))))

(defn process-statuses
  [favorites model]
  (let [missing-statuses-ids (get-missing-statuses-ids favorites model)
        remaining-favorites (filter (in-ids-as-string-set missing-statuses-ids) favorites)]
    (when
      (and
        (pos? (count missing-statuses-ids))
        (pos? (count remaining-favorites)))
      (ensure-statuses-exist remaining-favorites model))))

(defn preprocess-statuses
  [statuses status-model member-model token-model]
  (when (pos? (count statuses))
    (process-authors-of-statuses statuses member-model token-model)
    (process-statuses statuses status-model)))

(defn get-author-by-id
  "Provide with a map of authors indexed by theirs Twitter ids
  in order to get a function returning an author matching a Twitter id passed as argument"
  [indexed-authors]
  (fn [status-id]
    ((keyword status-id) indexed-authors)))

(defn get-id-of-status-author
  "Get the Twitter id of the author of a status"
  [status]
  (let [{{author-id :id_str} :user} status]
    author-id))

(defn get-author-key-value
  "Get a pair made of a Twitter id and a value"
  [args]
  (let [twitter-id (:twitter-id args)
        key-value [(keyword twitter-id) args]]
    key-value))

(defn get-statuses-authors
  "Get authors of statuses"
  [favorites model]
  (let [author-ids (pmap get-id-of-status-author favorites)
        distinct-status-authors (find-members-having-ids author-ids model)
        indexed-authors (->> (map get-author-key-value distinct-status-authors)
                             (into {}))
        favorited-status-authors (map (get-author-by-id indexed-authors) author-ids)]
    (log/info (str "Found " (count favorited-status-authors) " ids of status authors"))
    favorited-status-authors))

(defn new-relationship
  [aggregate-id]
  (fn [status-id]
  (let [relationship {:status-id status-id :aggregate-id aggregate-id}]
    relationship)))

(defn new-relationships
  [aggregate new-statuses model status-model]
  (if (pos? (count new-statuses))
    (let [aggregate-id (:id aggregate)
          new-statuses-ids (map #(:id %) new-statuses)
          relationship-values (doall (map (new-relationship aggregate-id) new-statuses-ids))
          new-relationships (bulk-insert-status-aggregate-relationship
                              relationship-values
                              aggregate-id
                              model
                              status-model)
          total-new-relationships (count new-relationships)]
      {:new-relationships new-relationships
       :total-new-relationships total-new-relationships})
      {:new-relationships '()
       :total-new-relationships 0}))

(defn cache-statuses-along-with-authors
  "Ensure statuses and their authors are cached"
  [statuses screen-name aggregate {member-model :members
                       status-model :status
                       token-model :tokens
                       status-aggregate-model :status-aggregate}]
  (if (pos? (count statuses))
    (let [new-statuses (preprocess-statuses statuses status-model member-model token-model)
          {total-new-relationships :total-new-relationships
           new-relationships :new-relationships} (new-relationships
                                                   aggregate
                                                   new-statuses
                                                   status-aggregate-model
                                                   status-model)]
      (if (pos? total-new-relationships)
        (log/info (str "There are " total-new-relationships " new relationships between aggregate \""
                       (:name aggregate) "\" for \"" screen-name "\" and " (count new-statuses) " statuses"))
        (log/info (str "There are no new relationships for aggregate \"" (:name aggregate) "\"")))
    new-relationships)
    '()))
