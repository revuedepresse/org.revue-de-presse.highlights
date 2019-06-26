; About gen-class examples
; @see https://clojure.org/reference/compilation#_gen_class_examples
(ns review.core
  (:require [clojure.tools.logging :as log]
            [command.analysis.frequency :as analysis-frequencies]
            [command.generate-keywords :as keywords]
            [command.update-members-props :as members]
            [command.unarchive-statuses :as unarchived-statuses]
            [command.save-highlights :as highlights]
            [command.collect-status-identities :as status-identities]
            [command.collect-timely-statuses :as timely-statuses])
  (:use [korma.db]
        [twitter.api-client]
        [amqp.message-handler]
        [command.recommend-subscriptions])
  (:gen-class))

(log/log-capture! "review")

(defn add-frequencies-of-publication-for-member-subscriptions
  [args]
  (let [[screen-name sample-label week year] args
        week (Long/parseLong week)
        year (Long/parseLong year)]
    (try
      (analysis-frequencies/add-frequencies-of-publication-for-member-subscriptions
        screen-name
        {:sample-label sample-label
         :year         year
         :week         week})
      (catch Exception e
        (log/error (.getMessage e))))))

(defn consume-amqp-messages
  [args]
  (let [[queue messages consumers] args
        total-messages (if (nil? messages)
                         100
                         (Long/parseLong messages))
        parallel-consumers (if (nil? consumers)
                             1
                             (Long/parseLong consumers))]
    (try
      (consume-messages (keyword queue) total-messages parallel-consumers)
      (catch Exception e (log/error
                           (str "An error occurred with message: " (.getMessage e)))))))

(defn recommend-subscriptions
  [args]
  (let [[screen-name] args]
    (recommend-subscriptions-from-member-subscription-history screen-name)))

(defn save-highlights
  [args]
  (let [[date] args]
    (cond
      (nil? date)
      (highlights/save-today-highlights)
      (= 0 (count args))
      (highlights/save-highlights date)
      :else
      (apply highlights/save-highlights args))))

(defn save-highlights-for-all-aggregates
  [args]
  (let [[date] args]
    (highlights/save-highlights-for-all-aggregates date)))

(defn update-members-descriptions-urls
  []
  (members/update-members-descriptions-urls))

(defn unarchive-statuses
  [args]
  (let [[week year] args
        year (Long/parseLong year)
        week (Long/parseLong week)]
    (unarchived-statuses/unarchive-statuses week year)))

(defn collect-timely-statuses-for-member-subscriptions
  [args]
  (let [[member] args]
    (timely-statuses/collect-timely-statuses-for-member-subscriptions member)))

(defn collect-timely-statuses-from-aggregates
  [args]
  (let [[reverse-order] args]
    (timely-statuses/collect-timely-statuses-from-aggregates reverse-order)))

(defn collect-timely-statuses-from-aggregate
  [args]
  (let [[aggregate-name] args]
    (timely-statuses/collect-timely-statuses-from-aggregate aggregate-name)))

(defn collect-timely-statuses-for-member
  [args]
  (let [[member] args]
    (timely-statuses/collect-timely-statuses-for-member member)))

(defn generate-keywords-from-statuses
  [args]
  (let [[date] args]
    (if (> (count args) 1)
      (keywords/generate-keywords-for-all-aggregates
        date
        {:week (Long/parseLong (first args))
         :year (Long/parseLong (second args))})
      (keywords/generate-keywords-for-all-aggregates date))))

(defn generate-keywords-for-aggregate
  [args]
  (let [[aggregate-name] args]
    (keywords/generate-keywords-for-aggregate aggregate-name)))

(defn generate-keywords-for-aggregates-sharing-name
  [args]
  (let [[aggregate-name] args]
    (keywords/generate-keywords-for-aggregates-sharing-name aggregate-name)))

(defn collect-status-identities-for-aggregates
  [args]
  (let [[aggregate-name] args]
    (status-identities/collect-status-identities-for-aggregates aggregate-name)))

(defn collect-status-identities-for
  [args]
  (let [[aggregate-id week year] args]
    (status-identities/collect-status-identities-for {:aggregate-id aggregate-id
                                                      :week           week
                                                      :year           year})))

(defn record-popularity-of-highlights
  [args]
  (let [[date] args]
    (highlights/record-popularity-of-highlights date)))

(defn who-publish-the-most-for-each-day-of-week
  [args]
  (let [[label] args]
    (analysis-frequencies/who-publish-the-most-for-each-day-of-week label)))

(defn execute-command
  [name args]
  (let [s (symbol (str "review.core/" name))
        f (resolve s)]
    (if f
      (try
        (apply f [args])
        (catch Exception e
          (log/error (.getMessage e))))
      (log/info "Invalid command"))))

(defn -main
  "Command dispatch application"
  [name & args]
  (try
    (execute-command name args)
    (catch Exception e
      (log/error (.getMessage e)))))

