; About gen-class examples
; @see https://clojure.org/reference/compilation#_gen_class_examples
(ns review.core
  (:require [clojure.tools.logging :as log])
  (:use [korma.db]
        [twitter.api-client]
        [amqp.message-handler]
        [amqp.recommendation_handler])
  (:gen-class))

(defn -main
  "Command dispatch application (AMQP message consumption / recommendation)"
  [name & args]
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
          (catch Exception e (log/error (str "An error occurred with message: " (.getMessage e))))))
    (= name "recommend-subscriptions")
      (let [[screen-name] args]
          (recommend-subscriptions-from-member-subscription-history screen-name))
    :else
      (log/info "Invalid command")))
