(ns command.analysis.frequency
  (:require [clj-time.coerce :as c]
            [clj-time.core :as time]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:use [twitter.date]
        [repository.entity-manager]
        [repository.status]))

(defn inc-frequency-of-publication-for-day-of-week
  [per-day-of-week-frequency day-of-week]
  (let [day-index (dec day-of-week)
        day-count (nth per-day-of-week-frequency day-index)
        one-more-day-count (inc day-count)
        new-frequency (assoc per-day-of-week-frequency day-index one-more-day-count)]
    new-frequency))

(defn inc-frequency-of-publication-for-hour-of-day
  [per-hour-of-day-frequency hour]
  (let [count (nth per-hour-of-day-frequency hour)
        one-more-hour-added-to-count (inc count)
        new-frequency (assoc per-hour-of-day-frequency hour one-more-hour-added-to-count)]
    new-frequency))

(defn get-publication-day-frequency-analysis
  [statuses]
  (let [per-day-of-week-frequency (apply vector (take 7 (constantly 0)))
        days-of-week (pmap
                       #(time/day-of-week (c/from-long (:created-at %)))
                       statuses)]
    (try
      (reduce inc-frequency-of-publication-for-day-of-week per-day-of-week-frequency days-of-week)
      (catch Exception e
        (log/error (.getMessage e))))))

(defn get-publication-hour-frequency-analysis
  [statuses]
  (let [per-hour-of-day-frequency (apply vector (take 24 (iterate (constantly 0) 0)))
        hour-of-day (pmap
                       #(time/hour (c/from-long (:created-at %)))
                       statuses)]
    (try
      (reduce inc-frequency-of-publication-for-hour-of-day per-hour-of-day-frequency hour-of-day)
      (catch Exception e
        (log/error (.getMessage e))))))

(defn analyze-frequency-of-status-publication-by-day-of-week
  [screen-name]
  (let [{status-model :status} (get-entity-manager (:database env))
        statuses (find-statuses-by-screen-name screen-name status-model)
        publication-frequency (get-publication-day-frequency-analysis statuses)
        divider (apply max publication-frequency)
        frequencies (map #(float (/ % divider)) publication-frequency)]
    (println frequencies)
    frequencies))

(defn analyze-frequency-of-status-publication-by-hour-of-day
  [screen-name]
  (let [{status-model :status} (get-entity-manager (:database env))
        statuses (find-statuses-by-screen-name screen-name status-model)
        publication-frequency (get-publication-hour-frequency-analysis statuses)
        divider (apply max publication-frequency)
        frequencies (map #(float (/ % divider)) publication-frequency)]
    (println frequencies)
    frequencies))

