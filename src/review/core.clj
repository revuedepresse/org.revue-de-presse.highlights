; About gen-class examples
; @see https://clojure.org/reference/compilation#_gen_class_examples
(ns review.core
  (:require [clojure.tools.logging :as log]
            [command.analysis.frequency :as analysis-frequencies]
            [command.generate-keywords :as keywords]
            [command.navigation :as navigation]
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

(defn command-add-frequencies-of-publication-for-member-subscriptions
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

(defn ^{:requires [:queue :messages :consumers]} command-consume-amqp-messages
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

(defn ^{:requires [:aggregate-id :week :year]} command-collect-status-identities-for
  [args]
  (let [[aggregate-id week year] args]
    (status-identities/collect-status-identities-for {:aggregate-id aggregate-id
                                                      :week         week
                                                      :year         year})))

(defn ^{:requires [:aggregate-name]} command-collect-status-identities-for-aggregates
  [args]
  (let [[aggregate-name] args]
    (status-identities/collect-status-identities-for-aggregates aggregate-name)))

(defn ^{:requires [:screen-name]} command-collect-timely-statuses-for-member-subscriptions
  [args]
  (let [[screen-name] args]
    (timely-statuses/collect-timely-statuses-for-member-subscriptions screen-name)))

(defn ^{:requires [:reverse-order]} command-collect-timely-statuses-from-aggregates
  [args]
  (let [[reverse-order] args]
    (timely-statuses/collect-timely-statuses-from-aggregates reverse-order)))

(defn ^{:requires [:aggregate-name]} command-collect-timely-statuses-from-aggregate
  [args]
  (let [[aggregate-name] args]
    (timely-statuses/collect-timely-statuses-from-aggregate aggregate-name)))

(defn ^{:requires [:screen-name]} command-collect-timely-statuses-for-member
  [args]
  (let [[screen-name] args]
    (timely-statuses/collect-timely-statuses-for-member screen-name)))

(defn ^{:requires [:date]} command-generate-keywords-from-statuses
  [args]
  (let [[date] args]
    (if (> (count args) 1)
      (keywords/generate-keywords-for-all-aggregates
        date
        {:week (Long/parseLong (first args))
         :year (Long/parseLong (second args))})
      (keywords/generate-keywords-for-all-aggregates date))))

(defn ^{:requires [:aggregate-name]} command-generate-keywords-for-aggregate
  [args]
  (let [[aggregate-name] args]
    (keywords/generate-keywords-for-aggregate aggregate-name)))

(defn ^{:requires [:aggregate-name]} command-generate-keywords-for-aggregates-sharing-name
  [args]
  (let [[aggregate-name] args]
    (keywords/generate-keywords-for-aggregates-sharing-name aggregate-name)))

(defn ^{:requires []} command-list-aggregates
  []
  (navigation/list-aggregates))

(defn ^{:requires [:screen-name]} command-list-aggregates-containing-members
  [args]
  (let [[screen-name] args]
    (navigation/list-aggregates-containing-member screen-name)))

(defn ^{:requires [:screen-name]} command-list-member-statuses
  [args]
  (let [[screen-name] args]
    (navigation/list-member-statuses screen-name)))

(defn ^{:requires [:aggregate-name]} command-list-aggregate-statuses
  [args]
  (let [[aggregate-name] args]
    (navigation/list-aggregate-statuses aggregate-name)))

(defn ^{:requires [:aggregate-name]} command-list-keywords-by-aggregate
  [args]
  (let [[aggregate-name] args]
    (navigation/list-keywords-by-aggregate aggregate-name)))

(defn ^{:requires [:aggregate-name]} command-list-mentions-by-aggregate
  [args]
  (let [[aggregate-name] args]
    (navigation/list-mentions-by-aggregate aggregate-name)))

(defn ^{:requires [:aggregate-name]} command-list-members-in-aggregate
  [args]
  (let [[aggregate-name] args]
    (navigation/list-members-in-aggregate aggregate-name)))

(defn ^{:requires [:screen-name]} command-recommend-subscriptions
  [args]
  (let [[screen-name] args]
    (recommend-subscriptions-from-member-subscription-history screen-name)))

(defn ^{:requires [:date]} command-record-popularity-of-highlights
  [args]
  (let [[date] args]
    (highlights/record-popularity-of-highlights date)))

(defn ^{:requires [:date]} command-save-highlights
  [args]
  (let [[date] args]
    (cond
      (nil? date)
      (highlights/save-today-highlights)
      (= 0 (count args))
      (highlights/save-highlights date)
      :else
      (apply highlights/save-highlights args))))

(defn ^{:requires [:date]} command-save-highlights-for-all-aggregates
  [args]
  (let [[date] args]
    (highlights/save-highlights-for-all-aggregates date)))

(defn ^{:requires [:week :year]} command-unarchive-statuses
  [args]
  (let [[week year] args
        year (Long/parseLong year)
        week (Long/parseLong week)]
    (unarchived-statuses/unarchive-statuses week year)))

(defn ^{:requires [:screen-name]} command-show-member-descriptions
  "Show member bio"
  [args]
  (let [[screen-name] args]
    (navigation/get-member-description screen-name)))

(defn ^{:requires [:any]} command-show-latest-evaluation
  [& args]
  (let [coll (if (and
                   (some? args)
                   (pos? (count args)))
               (:result (first args))
               '())
        formatter (if (some? coll)
                    (fn [m]
                      (str
                        (clojure.string/join
                          "\n"
                          (map
                            #(str (name %) ": " (get m %))
                            (sort (keys (first coll))))
                          )
                        "\n"))
                    identity)]
    (navigation/print-formatted-string
      formatter
      coll
      {:no-wrap false
       :sep     "------------------"})
    (if (some? coll)
      args
      {:result '()})))

(defn ^{:requires []} command-update-members-descriptions-urls
  []
  (members/update-members-descriptions-urls))

(defn ^{:requires [:label]} command-who-publish-the-most-for-each-day-of-week
  [args]
  (let [[label] args]
    (analysis-frequencies/who-publish-the-most-for-each-day-of-week label)))

(defn execute-command
  [name args]
  (let [s (symbol (str "review.core/command" name))
        f (resolve s)]
    (if f
      (navigation/try-running-command f args)
      (loop [print-menu true
             result-map nil]
        (navigation/print-menu-when print-menu)
        (let [ns-commands (navigation/find-ns-symbols result-map)
              input (if (navigation/should-quit-from-last-result result-map)
                      "q"
                      (read-line))]
          (cond
            (= input "q") (println "bye")
            (= input "h") (do
                            (navigation/print-help ns-commands)
                            (recur false nil))
            (navigation/is-valid-command-index input (count ns-commands)) (do
                                                                            (recur
                                                                              false
                                                                              (navigation/run-command-indexed-at
                                                                                (Long/parseLong input)
                                                                                ns-commands
                                                                                result-map)))
            :else (do
                    (println (str "\nInvalid command: \"" input "\""))
                    (recur false result-map))))))))

(defn -main
  "Command dispatch application"
  [& args]
  (try
    (execute-command (first args) (rest args))
    (catch Exception e
      (log/error (.getMessage e)))))

