(ns command.save-highlights
  (:require [environ.core :refer [env]]
            [clj-uuid :as uuid]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clj-time.predicates :as pr]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.local :as l]
            [clj-time.format :as f]
            [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [repository.aggregate]
        [repository.highlight]
        [repository.status-popularity]
        [repository.timely-status]
        [twitter.date]
        [twitter.status]
        [utils.error-handler :as error-handler]))

(def highlights-date-formatter (f/formatter "yyyy-MM-dd"))

(defn extract-highlight-props
  [aggregate]
  (fn [document]
    (try
      (let [api-document (:api-document document)
            decoded-document (json/read-str api-document)
            retweet-publication-date-time (if (some? (get decoded-document "retweeted_status"))
                                            (get (get decoded-document "retweeted_status") "created_at")
                                            nil)
            highlight-props {:id                                (uuid/to-string (uuid/v1))
                             :member-id                         (:member-id document)
                             :status-id                         (:status-id document)
                             :aggregate-id                      (:id aggregate)
                             :aggregate-name                    (:name aggregate)
                             :is-retweet                        (some? (get decoded-document "retweeted_status"))
                             :publication-date-time             (:publication-date-time document)
                             :retweeted-status-publication-date (if (some? retweet-publication-date-time)
                                                                  (f/unparse
                                                                    mysql-date-formatter
                                                                    (c/from-long
                                                                      (c/to-long
                                                                        (f/parse date-formatter retweet-publication-date-time))))
                                                                  nil)
                             :total-retweets                    (get decoded-document "retweet_count")
                             :total-favorites                   (get decoded-document "favorite_count")}]
        (log/info (str "Prepared highlight for member #" (:member-id highlight-props)
                       " and status #" (:status-id highlight-props)))
        highlight-props)
      (catch Exception e
        (error-handler/log-error
          e
          "When extracting highlight props: ")))))

(defn record-popularity-of-highlights-batch
  [highlights checked-at {status-popularity :status-popularity
                          tokens            :tokens}]
  (let [filtered-highlights (remove #(nil? %) highlights)
        statuses (fetch-statuses filtered-highlights tokens)
        statuses (remove #(nil? %) statuses)
        status-popularity-props (doall
                                  (pmap
                                    #(assoc
                                       {:status-id (:id %)}
                                       :total-retweets (:retweet_count %)
                                       :checked-at checked-at
                                       :total-favorites (:favorite_count %))
                                    statuses))
        status-popularities (bulk-insert-of-status-popularity-props status-popularity-props checked-at status-popularity)]
    (doall
      (map
        #(log/info (str "Saved popularity of status #" (:status-id %)))
        status-popularities))
    status-popularities))

(defn record-popularity-of-highlights
  ([date]
   (record-popularity-of-highlights date (:press (edn/read-string (:aggregate env)))))
  ([date aggregate-name]
   (let [models (get-entity-manager (:database env))
         checked-at (f/unparse mysql-date-formatter
                               (c/from-long
                                 (c/to-long
                                   (f/unparse date-hour-formatter (l/local-now)))))
         pad (take 300 (iterate (constantly nil) nil))
         highlights (find-highlights-for-aggregate-published-at date aggregate-name)
         highlights-partitions (partition 300 300 (vector pad) highlights)
         total-partitions (count highlights-partitions)]
     (loop [partition-index 0]
       (when (< partition-index total-partitions)
         (try
           (record-popularity-of-highlights-batch (nth highlights-partitions partition-index) checked-at models)
           (catch Exception e
             (error-handler/log-error e (str "Could not record popularity of highlights: "))))
         (recur (inc partition-index)))))))

(defn record-popularity-of-highlights-for-all-aggregates
  ([date]
   ; opening database connection beforehand
   (let [_ (get-entity-manager (:database env))
         excluded-aggregate (:press (edn/read-string (:aggregate env)))]
     (record-popularity-of-highlights-for-all-aggregates date excluded-aggregate)))
  ([date excluded-aggregate]
   (let [aggregates (find-aggregate-having-publication-from-date date excluded-aggregate)]
     (->> aggregates
          (map #(record-popularity-of-highlights date (:aggregate-name %)))
          doall))))

(defn record-popularity-of-highlights-for-main-aggregate
  ([date]
   ; opening database connection beforehand
   (let [_ (get-entity-manager (:database env))
         aggregate-name (:press (edn/read-string (:aggregate env)))]
     (record-popularity-of-highlights-for-main-aggregate date aggregate-name :include-aggregate)))
  ([date aggregate-name & [include-aggregate]]
   (let [aggregates (find-aggregate-having-publication-from-date date aggregate-name include-aggregate)]
     (->> aggregates
          (map #(record-popularity-of-highlights date (:aggregate-name %)))
          doall))))

(defn try-finding-highlights-by-status-ids
  [models]
  (fn [status-ids]
    (try
      (find-highlights-having-ids status-ids (:highlight models) (:member models) (:status models))
      (catch Exception e
        (error-handler/log-error e)))))

(defn bulk-insert-highlights-from-statuses
  [statuses-ids aggregate models]
  (let [statuses (find-statuses-by-ids statuses-ids)
        filtered-statuses (filter-out-known-statuses (try-finding-highlights-by-status-ids models) statuses)
        highlights-props (map (extract-highlight-props aggregate) filtered-statuses)
        new-highlights (bulk-insert-new-highlights highlights-props (:highlight models) (:member models) (:status models))]
    (log/info (str "There are " (count new-highlights) " new highlights"))
    new-highlights))

(defn try-insert-highlights-from-statuses
  [aggregate models]
  (fn [statuses-ids]
    (try
      (bulk-insert-highlights-from-statuses statuses-ids (first aggregate) models)
      (catch Exception e
        (error-handler/log-error e)))))

(defn save-highlights-from-date-for-aggregate
  [date aggregate]
  (let [{aggregate-model :aggregate :as models} (get-entity-manager (:database env))
        aggregate-name (if aggregate
                         aggregate
                         (:press (edn/read-string (:aggregate env))))
        aggregate (find-aggregate-by-name aggregate-name (some? aggregate) aggregate-model)
        statuses-ids (doall
                        (map
                           :status-id
                           (find-timely-statuses-by-aggregate-and-publication-date aggregate-name date)))
        ; @see https://clojuredocs.org/clojure.core/partition#example-542692d4c026201cdc327028
        ; about the effect of passing step and pad arguments
        pad (take 100 (iterate (constantly nil) nil))
        statuses-ids-chunk (partition 100 100 pad statuses-ids)]
    (log/info (str "About to insert at most " (count statuses-ids-chunk) " highlights chunk(s) from statuses ids"))
    (doall
      (map
        (try-insert-highlights-from-statuses aggregate models)
        statuses-ids-chunk))))

(defn save-highlights
  ([]
   (save-highlights nil))
  ([date]
   (save-highlights-from-date-for-aggregate date nil))
  ([month year]
   (let [month (Long/parseLong month)
         year (Long/parseLong year)
         days (take 31 (iterate inc 1))
         month-days (map #(t/date-time year month %) days)
         last-date-time (first (filter #(pr/last-day-of-month? %) month-days))
         last-day (t/day last-date-time)]
     (loop [day 1]
       (when (<= day last-day)
         (save-highlights (f/unparse highlights-date-formatter (t/date-time year month day)))
         (recur (inc day)))))))

(defn save-today-highlights
  []
  (save-highlights))

(defn save-highlights-for-all-aggregates
  [date]
  (let [_ (get-entity-manager (:database env))
        aggregate-name (:press (edn/read-string (:aggregate env)))
        aggregates (find-aggregate-having-publication-from-date date aggregate-name)]
    (doall (map #(save-highlights-from-date-for-aggregate date (:aggregate-name %)) aggregates))))

(defn save-highlights-for-main-aggregate
  [date]
  (let [_ (get-entity-manager (:database env))
        aggregate-name (:press (edn/read-string (:aggregate env)))
        aggregates (find-aggregate-having-publication-from-date date aggregate-name :include-aggregate)]
    (doall (map #(save-highlights-from-date-for-aggregate date (:aggregate-name %)) aggregates))))
