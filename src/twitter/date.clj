(ns twitter.date
  (:require [clj-time.coerce :as c]
            [clj-time.format :as f]
            [clj-time.local :as l]
            [clj-time.core :as t])
  (:import java.util.Locale))

(def date-formatter (f/with-locale (f/formatter "EEE MMM dd HH:mm:ss Z yyyy") Locale/ENGLISH))
(def db-date-formatter (f/with-locale (f/formatter "EEE, dd MMM yyyy HH:mm:ss Z") Locale/ROOT))
(def date-hour-formatter (f/formatters :date-hour))

(defn get-date-properties
  [date]
  (let [parsed-publication-date (c/to-long (f/parse date-formatter date))]
    {:parsed-publication-date parsed-publication-date
     :mysql-formatted-publication-date (f/unparse db-date-formatter (c/from-long parsed-publication-date))}))

(defn get-status-formatted-dates
  [status]
  (let [formatted-date (get-date-properties (:created_at status))]
    formatted-date))

(defn get-publication-dates-of-statuses
  [statuses]
  (pmap get-status-formatted-dates statuses))


(defn get-timestamp
  []
  (c/to-long (l/local-now)))

(defn get-time-range
  [timestamp]
  (let [now (get-timestamp)
        date (c/from-long timestamp)
        five-minutes-ago (c/from-long (- now (* 60 5 1000)))
        ten-minutes-ago (c/from-long (- now (* 60 10 1000)))
        thirty-minutes-ago (c/from-long (- now (* 60 30 1000)))
        one-day-ago (c/from-long (- now (* 3600 24 1000)))
        one-week-ago (c/from-long (- now (* 7 3600 24 1000)))]
    (cond
      (t/after? date five-minutes-ago)
      0
      (t/after? date ten-minutes-ago)
      1
      (t/after? date thirty-minutes-ago)
      2
      (t/after? date one-day-ago)
      3
      (t/after? date one-week-ago)
      4
      :else
      5)))
