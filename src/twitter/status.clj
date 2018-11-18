(ns twitter.status
  (:require [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [twitter.api-client])
  (:import java.util.Locale))

(def date-formatter (f/with-locale (f/formatter "EEE MMM dd HH:mm:ss Z yyyy") Locale/ENGLISH))
(def mysql-date-formatter (f/formatters :mysql))

(defn get-date-properties
  [date]
  (let [parsed-publication-date (c/to-long (f/parse date-formatter date))]
    {:parsed-publication-date parsed-publication-date
     :mysql-formatted-publication-date (f/unparse mysql-date-formatter (c/from-long parsed-publication-date))}))

(defn get-status-formatted-dates
  [favorite]
  (let [formatted-date (get-date-properties (:created_at favorite))]
    formatted-date))

(defn get-publication-dates-of-statuses
  [statuses]
  (pmap get-status-formatted-dates statuses))

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
    (when (pos? total-statuses)
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
