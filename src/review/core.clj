; About gen-class examples
; @see https://clojure.org/reference/compilation#_gen_class_examples
(ns review.core
  (:require [clojure.tools.logging :as log])
  (:use [korma.db]
        [twitter.api-client]
        [amqp.message-handler]
        [command.generate-timely-statuses]
        [command.update-members-props]
        [command.save-highlights]
        [command.recommend-subscriptions]
        [command.unarchive-statuses])
  (:gen-class))

(log/log-capture! "review")

(defn execute-command
  [name args]
  (cond
     (= name "consume-amqp-messages")
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
                                (str "An error occurred with message: " (.getMessage e))))))
     (= name "recommend-subscriptions")
       (let [[screen-name] args]
         (recommend-subscriptions-from-member-subscription-history screen-name))
     (= name "update-members-descriptions-urls")
       (update-members-descriptions-urls)
     (= name "unarchive-statuses")
       (let [[week year] args
             year (Long/parseLong year)
             week (Long/parseLong week)]
         (unarchive-statuses week year))
     (= name "generate-timely-statuses")
       (let [[week year] args
             year (Long/parseLong year)
             week (Long/parseLong week)]
         (generate-timely-statuses week year))
     (= name "save-highlights")
       (let [[date] args]
         (cond
           (nil? date)
            (save-today-highlights)
           (= 0 (count args))
            (save-highlights date)
           :else
            (apply save-highlights args)))
     :else
     (log/info "Invalid command")))

(defn -main
  "Command dispatch application (AMQP message consumption / recommendation)"
  [name & args]
  (try
    (execute-command name args)
    (catch Exception e (log/error (.getMessage e)))))

