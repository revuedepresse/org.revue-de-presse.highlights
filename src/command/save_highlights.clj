(ns command.save-highlights
  (:require [environ.core :refer [env]]
            [clj-uuid :as uuid]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clj-time.predicates :as pr]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [repository.highlight]
        [twitter.status]))

(def highlights-date-formatter (f/formatter "yyyy-MM-dd"))

(defn extract-total-props
  [document]
  (let [api-document (:api-document document)
        decoded-document (json/read-str api-document)
        highlight-props {:id (uuid/to-string (uuid/v1))
                         :member-id (:member-id document)
                         :status-id (:status-id document)
                         :publication-date-time (:publication-date-time document)
                         :total-retweets (get decoded-document "retweet_count")
                         :total-favorites (get decoded-document "favorite_count")}]
    (log/info (str "Prepared highlight for member #" (:member-id highlight-props)
                   " and status #" (:status-id highlight-props)))
  highlight-props))

(defn save-highlights
  ([]
    (save-highlights nil))
  ([date]
    (let [{highlight-model :highlight
           status-model :status
           member-model :members} (get-entity-manager (:database env))
          press-aggregate-name (:press (edn/read-string (:aggregate env)))
          statuses (find-statuses-for-aggregate press-aggregate-name date)
          find #(find-highlights-having-ids % highlight-model member-model status-model)
          filtered-statuses (filter-out-known-statuses find statuses)
          highlights-props (doall (map extract-total-props filtered-statuses))
          new-highlights (bulk-insert-new-highlights highlights-props highlight-model member-model status-model)]
      (log/info (str "There are " (count new-highlights) " new highlights"))
     new-highlights))
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
