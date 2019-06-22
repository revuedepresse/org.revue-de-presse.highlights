(ns command.collect-status-identities
  (:require
    [clojure.tools.logging :as log]
    [clj-time.local :as l]
    [clj-time.format :as f]
    [clj-time.coerce :as c]
    [clojure.data.json :as json]
    [environ.core :refer [env]]
    [clj-time.core :as time]
    [repository.status-identity :as status-identity])
  (:use
    [repository.aggregate]
    [twitter.date]
    [repository.entity-manager]))

(defn assoc-publication-date
  [props]
  (let [mysql-formatted-publication-date (f/unparse
                                           mysql-date-formatter
                                           (c/from-long (:publication-date-time props)))
        props-without-publication-date (dissoc props :publication-date-time)
        props-having-publication-date (assoc props-without-publication-date :publication-date-time mysql-formatted-publication-date)]
    props-having-publication-date))

(defn assoc-twitter-id
  [props]
  (let [json-decoded-document (json/read-str (:status-api-document props))
        props-minus-document (dissoc props :status-api-document)
        props-having-twitter-id (assoc props-minus-document :twitter-id (get json-decoded-document "id_str"))]
    props-having-twitter-id))

(defn assoc-missing-props
  [props]
  (let [props (->
                props
                assoc-twitter-id
                assoc-publication-date)]
    props))

(defn decode-status-documents-for-week-and-year
  [aggregate-id week year db]
  (let [_ (log/info (str "About to find member identities for "
                         "aggregate #" aggregate-id ", year \""
                         year "\" and week #"
                         week))
        aggregate-member-identities (try
                                      (status-identity/find-by-aggregate-id aggregate-id week year db)
                                      (catch Exception e
                                        (log/error (.getMessage e))))
        props (doall
                (pmap assoc-missing-props aggregate-member-identities))
        deduplicated-props (dedupe (sort-by #(str (:member-identity %) (:publication-date-time %)) props))]
    deduplicated-props))

(defn is-subset-of
  [props-set]
  (fn [props]
    (let [twitter-id (:twitter-id props)]
      (clojure.set/subset? #{twitter-id} props-set))))

(defn decode-available-documents
  [aggregate-id week year {read-db  :read-db
                           write-db :write-db} models]
  (let [_ (log/info (str "About to count existing status identities for "
                         "aggregate #" aggregate-id ", year \""
                         year "\" and week #" week))
        total-member-identities (status-identity/count-for-aggregate-id aggregate-id week year read-db)
        props (if (pos? total-member-identities)
                (decode-status-documents-for-week-and-year aggregate-id week year read-db)
                (do
                  (log/info (str "There are no status identities to be collected for "
                                 "aggregate #" aggregate-id ", year \""
                                 year "\" and week #" week))
                  '()))
        total-status-identities (count props)
        _ (when (pos? (count props))
            (let [twitter-ids (map #(:twitter-id %) props)
                  existing-props (status-identity/find-status-identity-by-twitter-ids twitter-ids models)
                  filtered-props (doall
                                   (remove (is-subset-of (set existing-props)) props))]
              (status-identity/bulk-insert-status-identities
                filtered-props
                {:aggregate-id aggregate-id
                 :week         week
                 :year         year}
                write-db)))]
    total-status-identities))

(defn decode-status-documents
  [aggregate-id {read-db :read-db :as databases} models]
  (let [_ (log/info (str "About to get year since when existing status identities could be collected for "
                         "aggregate #" aggregate-id))
        min-week-year (status-identity/get-min-week-year-for-aggregate-id aggregate-id read-db)
        since-year (:since-year min-week-year)
        now (l/local-now)
        remaining-years (inc (- (time/year now) since-year))
        years (take remaining-years (iterate inc since-year))
        for-each-week-of (fn
                           [aggregate-id year]
                           (let [weeks (take 53 (iterate inc 0))]
                             (doall
                               (map
                                 #(decode-available-documents aggregate-id % year databases models)
                                 weeks))))
        decoded-statuses (doall
                           (pmap #(for-each-week-of aggregate-id %) years))]
    decoded-statuses))

(defn collect-status-identities-for-aggregates
  [aggregate-name]
  (let [{write-db :connection} (get-entity-manager (:database env))
        {read-db :connection :as models} (get-entity-manager (:database-read env) {:is-read-connection true})
        aggregates (find-aggregates-sharing-name aggregate-name read-db)
        _ (doall
            (pmap
              #(decode-status-documents (:aggregate-id %) {:read-db  read-db
                                                           :write-db write-db} models)
              aggregates))]))