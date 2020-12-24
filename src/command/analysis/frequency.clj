(ns command.analysis.frequency
  (:require [clj-time.coerce :as c]
            [clj-time.local :as l]
            [clj-time.format :as f]
            [clj-time.core :as time]
            [clojure.data.json :as json]
            [environ.core :refer [env]]
            [utils.error-handler :as error-handler])
  (:use [twitter.date]
        [repository.entity-manager]
        [repository.aggregate]
        [repository.analysis.publication-frequency]
        [repository.analysis.sample]
        [repository.member]
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
  (let [per-day-of-week-frequency (apply vector (take 7 (iterate (constantly 0) 0)))
        days-of-week (pmap
                       #(time/day-of-week (c/from-long (:created-at %)))
                       statuses)]
    (try
      (reduce inc-frequency-of-publication-for-day-of-week per-day-of-week-frequency days-of-week)
      (catch Exception e
        (error-handler/log-error e)))))

(defn get-publication-hour-frequency-analysis
  [statuses]
  (let [per-hour-of-day-frequency (apply vector (take 24 (iterate (constantly 0) 0)))
        hour-of-day (pmap
                      #(time/hour (c/from-long (:created-at %)))
                      statuses)]
    (try
      (reduce inc-frequency-of-publication-for-hour-of-day per-hour-of-day-frequency hour-of-day)
      (catch Exception e
        (error-handler/log-error e)))))

(defn analyze-frequency-of-status-publication
  [{frequency-analysis-getter :frequency-analysis-getter
    screen-name               :screen-name
    status-model              :status-model
    year                      :year
    week                      :week}]
  (let [finder (if (nil? week)
                 #(find-statuses-by-screen-name screen-name status-model)
                 #(find-statuses-by-week-and-author week year screen-name))
        statuses (finder)
        publication-frequency (frequency-analysis-getter statuses)
        divider (apply + publication-frequency)
        publication-frequencies-percentage (if (zero? divider)
                                             (map (constantly 0) publication-frequency)
                                             (map #(float (/ % divider)) publication-frequency))
        frequencies {:publication-frequencies            publication-frequency
                     :publication-frequencies-percentage publication-frequencies-percentage}]
    (println frequencies)
    frequencies))

(defn add-member-publication-frequencies
  [{models      :models
    sample      :sample
    screen-name :screen-name
    year        :year
    week        :week}]
  (let [{member-model                :members
         publication-frequency-model :publication-frequency
         sample-model                :sample
         status-model                :status} models
        member (first (find-member-by-screen-name screen-name member-model))
        per-hour-of-day-frequencies (analyze-frequency-of-status-publication
                                      {:frequency-analysis-getter get-publication-hour-frequency-analysis
                                       :screen-name               screen-name
                                       :status-model              status-model
                                       :year                      year
                                       :week                      week})
        per-day-of-week-frequencies (analyze-frequency-of-status-publication
                                      {:frequency-analysis-getter get-publication-day-frequency-analysis
                                       :screen-name               screen-name
                                       :status-model              status-model
                                       :year                      year
                                       :week                      week})
        props [{:per-hour-of-day            (json/write-str (:publication-frequencies per-hour-of-day-frequencies))
                :per-day-of-week            (json/write-str (:publication-frequencies per-day-of-week-frequencies))
                :per-hour-of-day-percentage (json/write-str (:publication-frequencies-percentage per-hour-of-day-frequencies))
                :per-day-of-week-percentage (json/write-str (:publication-frequencies-percentage per-day-of-week-frequencies))
                :updated-at                 (f/unparse db-date-formatter (l/local-now))
                :sample-id                  (:id sample)
                :member-id                  (:id member)}]
        frequencies (bulk-insert-from-props
                      props
                      publication-frequency-model
                      member-model
                      sample-model)]
    frequencies))

(defn add-frequencies-of-publication-for-member-subscriptions
  [screen-name & [{sample-label :sample-label
                   year         :year
                   week         :week}]]
  (let [{publication-frequency :publication-frequency
         member                :members
         sample                :sample :as models} (get-entity-manager (:database env))
        sample (bulk-insert-sample [{:label sample-label}] sample member publication-frequency)
        aggregates (get-member-aggregates-by-screen-name screen-name)
        _ (doall
            (pmap
              #(try
                 (add-member-publication-frequencies {:models      models
                                                      :sample      (first sample)
                                                      :screen-name (:screen-name %)
                                                      :year        year
                                                      :week        week})
                 (catch Exception e
                   (error-handler/log-error e)))
              aggregates))]))

(defn decode-publication-frequencies
  [publication-frequencies]
  (let [per-hour-of-day (json/read-str (:per-hour-of-day publication-frequencies))
        per-day-of-week (json/read-str (:per-day-of-week publication-frequencies))
        decoded-hours-of-day (assoc publication-frequencies :per-hour-of-day per-hour-of-day)
        decoded-days-of-week (assoc decoded-hours-of-day :per-day-of-week per-day-of-week)]
    decoded-days-of-week))

(defn get-frequency-of-unit-for-member
  ; The type of frequency being the numeric representation of the day of a week or the hour of a day
  [frequency-type frequency-k frequencies]
  (let [member-id (:member-id frequencies)
        screen-name (:screen-name frequencies)
        unit-frequency (nth (get frequencies frequency-k) frequency-type)]
    (assoc {:member-id   member-id
            :screen-name screen-name} frequency-k unit-frequency)))

(defn get-frequencies-for-day-of-week
  [day-of-week decoded-publication-frequencies]
  (map #(get-frequency-of-unit-for-member day-of-week :per-day-of-week %) decoded-publication-frequencies))

(defn get-frequencies-for-hour-of-day
  [hour-of-day decoded-publication-frequencies]
  (map #(get-frequency-of-unit-for-member hour-of-day :per-hour-of-day %) decoded-publication-frequencies))

(defn who-publish-the-most-for-each-day-of-week
  [label]
  (let [{publication-frequency-model :publication-frequency
         member-model                :members
         sample-model                :sample} (get-entity-manager (:database env))
        publication-frequencies (find-publication-frequencies-by-label
                                  label
                                  publication-frequency-model
                                  member-model
                                  sample-model)
        decoded-publication-frequencies (map decode-publication-frequencies publication-frequencies)
        days-of-week (take 7 (iterate inc 0))
        hours-of-day (take 24 (iterate inc 0))
        per-day-of-week-frequencies (map
                                      #(get-frequencies-for-day-of-week % decoded-publication-frequencies)
                                      days-of-week)
        sorted-per-day-of-week-frequencies (map
                                             #(sort-by :per-day-of-week %)
                                             per-day-of-week-frequencies)
        per-hour-of-day-frequencies (map
                                      #(get-frequencies-for-hour-of-day % decoded-publication-frequencies)
                                      hours-of-day)
        sorted-per-hour-of-day-frequencies (map
                                             #(sort-by :per-hour-of-day %)
                                             per-hour-of-day-frequencies)
        _ (doall (map-indexed
                   (fn
                     [day-of-week sorted-frequencies]
                     (let [frequency-props (last sorted-frequencies)
                           day (cond
                                 (= 0 day-of-week) "monday"
                                 (= 1 day-of-week) "tuesday"
                                 (= 2 day-of-week) "wednesday"
                                 (= 3 day-of-week) "thursday"
                                 (= 4 day-of-week) "friday"
                                 (= 5 day-of-week) "saturday"
                                 (= 6 day-of-week) "sunday")]
                       (println (str
                                  "@" (:screen-name frequency-props)
                                  " published the most on " day))))
                   sorted-per-day-of-week-frequencies))
        _ (doall (map-indexed
                   (fn
                     [hour-of-day sorted-frequencies]
                     (let [frequency-props (last sorted-frequencies)
                           hour (str "at " (mod (+ 2 hour-of-day) 24) " o'clock")]
                       (println (str
                                  "@" (:screen-name frequency-props)
                                  " published the most " hour))))
                   sorted-per-hour-of-day-frequencies))]
    {:per-day-of-week-frequencies sorted-per-hour-of-day-frequencies
     :per-hour-of-day-frequencies sorted-per-day-of-week-frequencies}))