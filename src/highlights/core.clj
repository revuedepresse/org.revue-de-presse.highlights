; About gen-class examples
; @see https://clojure.org/reference/compilation#_gen_class_examples
(ns highlights.core
  (:require [clojure.tools.logging :as log]
            [command.analysis.frequency :as analysis-frequencies]
            [command.generate-keywords :as keywords]
            [command.navigation :as navigation]
            [command.update-members-props :as members]
            [command.unarchive-statuses :as unarchived-statuses]
            [command.save-highlights :as highlights]
            [command.collect-status-identities :as status-identities]
            [command.collect-timely-statuses :as timely-statuses]
            [adaptor.database-navigation :as adaptor]
            [maintenance.migration :as migration]
            [clojure.string :as string]
            [utils.error-handler :as error-handler])
  (:use [korma.db]
        [twitter.api-client]
        [amqp.message-handler]
        [command.recommend-subscriptions])
  (:gen-class))

(log/log-capture! "highlights")

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
        (error-handler/log-error e)))))

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
      (catch Exception e (error-handler/log-error
                           e
                           "An error occurred with message: ")))))

(defn ^{:requires [:aggregate-id :week :year]} command-collect-status-identities-for
  [args]
  (let [[aggregate-id week year] args]
    (status-identities/collect-status-identities-for {:aggregate-id aggregate-id
                                                      :week         week
                                                      :year         year})))

(defn ^{:requires [:publishers-list]} command-collect-status-identities-for-aggregates
  [args]
  (let [[publishers-list] args]
    (status-identities/collect-status-identities-for-aggregates publishers-list)))

(defn ^{:requires [:screen-name]} command-collect-timely-statuses-for-member-subscriptions
  [args]
  (let [[screen-name] args]
    (timely-statuses/collect-timely-statuses-for-member-subscriptions screen-name)))

(defn ^{:requires [:letter]} command-collect-timely-statuses-from-aggregates
  [args]
  (let [[letter reverse-order] args]
    (timely-statuses/collect-timely-statuses-from-aggregates letter reverse-order)))

(defn ^{:requires [:publishers-list]} command-collect-timely-statuses-from-aggregate
  [args]
  (let [[publishers-list] args]
    (timely-statuses/collect-timely-statuses-from-aggregate publishers-list)))

(defn ^{:requires [:screen-name]} command-collect-timely-statuses-for-member
  [args]
  (let [[screen-name] args]
    (timely-statuses/collect-timely-statuses-for-member screen-name)))

(defn ^{:requires [:date]} command-generate-keywords-from-statuses
  [args]
  (let [[date] args]
    (cond
      (= (count args) 2) (keywords/generate-keywords-for-all-aggregates
                           date
                           {:week (Long/parseLong (first args))
                            :year (Long/parseLong (second args))})
      (= (count args) 3) (keywords/generate-keywords-for-all-aggregates
                           date
                           {:aggregate (Long/parseLong (first args))
                            :week      (Long/parseLong (second args))
                            :year      (Long/parseLong (nth args 2))})
      :else (keywords/generate-keywords-for-all-aggregates date))))

(defn ^{:requires []} command-generate-keywords-for-last-week-publishers
  [& args]
  (keywords/generate-keywords-for-last-week-publishers))

(defn ^{:requires [:publishers-list]} command-generate-keywords-for-aggregate
  [args]
  (let [[publishers-list] args]
    (keywords/generate-keywords-for-aggregate publishers-list)))

(defn ^{:requires [:publishers-list]} command-generate-keywords-for-aggregates-sharing-name
  [args]
  (let [[publishers-list] args]
    (keywords/generate-keywords-for-aggregates-sharing-name publishers-list)))

(defn ^{:requires []} command-list-alphabet-letters
  [& args]
  (adaptor/list-alphabet-letters))

(defn ^{:requires []} command-list-members-lists
  [& args]
  (adaptor/list-members-lists))

(defn ^{:requires [:publishers-list]} command-list-aggregate-statuses
  [args]
  (let [[publishers-list] args]
    (adaptor/list-aggregate-statuses publishers-list)))

(defn ^{:requires []} command-list-keyword-aggregates
  [& args]
  (adaptor/list-keyword-aggregates))

(defn ^{:requires [:screen-name]} command-list-members-lists-containing-members
  [args]
  (let [[screen-name] args]
    (adaptor/list-members-lists-containing-member screen-name)))

(defn ^{:requires []} command-list-highlights-since-a-month-ago
  [& args]
  (adaptor/list-highlights-since-a-month-ago))

(defn ^{:requires [:screen-name]} command-list-member-statuses
  [args]
  (let [[screen-name] args]
    (adaptor/list-member-statuses screen-name)))

(defn ^{:requires []} command-list-members-subscribing-to-lists
  [& args]
  (adaptor/list-members-subscribing-to-lists))

(defn ^{:requires []} command-list-members-which-subscriptions-have-been-collected
  [& args]
  (adaptor/list-members-which-subscriptions-have-been-collected))

(defn ^{:requires [:screen-name]} command-list-members-lists-of-subscriber-having-screen-name
  [args]
  (let [[screen-name] args]
    (adaptor/list-members-lists-of-subscriber-having-screen-name screen-name)))

(defn ^{:requires [:screen-name]} command-list-subscriptions-of-member-having-screen-name
  [args]
  (let [[screen-name] args]
    (adaptor/list-subscriptions-of-member-having-screen-name screen-name)))

(defn ^{:requires [:keyword]} command-list-statuses-containing-keyword
  [args]
  (let [[keyword] args]
    (adaptor/list-statuses-containing-keyword keyword)))

(defn ^{:requires [:publishers-list]} command-list-keywords-by-aggregate
  [args]
  (let [[publishers-list] args]
    (adaptor/list-keywords-by-aggregate publishers-list)))

(defn ^{:requires [:publishers-list]} command-list-mentions-by-aggregate
  [args]
  (let [[publishers-list] args]
    (adaptor/list-mentions-by-aggregate publishers-list)))

(defn ^{:requires [:publishers-list]} command-list-members-in-aggregate
  [args]
  (let [[publishers-list] args]
    (adaptor/list-members-in-aggregate publishers-list)))

(defn ^{:requires [:screen-name]} command-recommend-subscriptions
  [args]
  (let [[screen-name] args]
    (recommend-subscriptions-from-member-subscription-history screen-name)))

(defn ^{:requires [:date]} command-record-popularity-of-highlights
  [args]
  (let [[date] args]
    (highlights/record-popularity-of-highlights date)))

(defn ^{:requires [:date :publishers-list]} command-record-popularity-of-highlights-for-publishers-list
  [args]
  (let [[date publishers-list] args]
    (highlights/record-popularity-of-highlights date publishers-list)))

(defn ^{:requires [:date]} command-record-popularity-of-highlights-for-all-aggregates
  [args]
  (let [[date] args]
    (highlights/record-popularity-of-highlights-for-all-aggregates date)))

(defn ^{:requires [:date]} command-record-popularity-of-highlights-for-main-aggregate
  [args]
  (let [[date] args]
    (highlights/record-popularity-of-highlights-for-main-aggregate date)))

(defn ^{:requires [:date]} command-save-highlights-for-main-aggregate
  [args]
  (let [[date] args]
    (highlights/save-highlights-for-main-aggregate date)))

(defn ^{:requires [:date :publishers-list]} command-save-highlights-from-date-for-publishers-list
  [args]
  (let [[date publishers-list] args]
    (highlights/save-highlights-from-date-for-aggregate date publishers-list)))

(defn ^{:requires [:date]} command-save-highlights
  [args]
  (let [[date] args]
    (cond
      (nil? date) (highlights/save-today-highlights)
      (= 0 (count args)) (highlights/save-highlights date)
      :else (apply highlights/save-highlights args))))

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

(defn ^{:requires [:screen-name]} command-show-member-description
  "Show member bio"
  [args]
  (let [[screen-name] args]
    (adaptor/get-member-description screen-name)))

(defn ^{:requires [:any]} command-show-latest-evaluation
  [& args]
  (adaptor/render-latest-result-map args))

(defn ^{:requires []} command-update-members-descriptions-urls
  [& args]
  (members/update-members-descriptions-urls))

(defn ^{:requires [:label]} command-who-publish-the-most-for-each-day-of-week
  [args]
  (let [[label] args]
    (analysis-frequencies/who-publish-the-most-for-each-day-of-week label)))

(defn ^{:requires []} command-migrate-all-status-to-publications
  [& args]
  (migration/migrate-all-status-to-publications))

(defn execute-command
  [name args]
  (let [s (symbol (str "highlights.core/command-" name))
        f (resolve s)]
    (if f
      (navigation/try-running-command f args)
      (loop [print-menu true
             result-map nil]
        (navigation/print-menu-when print-menu)
        (when (and (pos? (count result-map))
                   (some-> result-map :result first :status-url))
          (println
            (string/join "\n\n"
                         (map
                           (:formatter result-map)
                           (:result result-map)))))
        (let [ret (navigation/handle-input result-map)]
          (when (some? ret)
            (recur
              (first ret)
              (second ret))))))))

(defn -main
  "Command dispatch application"
  [& args]
  (try
    (execute-command (first args) (rest args))
    (catch Exception e
      (error-handler/log-error e))))
