; About gen-class examples
; @see https://clojure.org/reference/compilation#_gen_class_examples
(ns review.core
  (:use [korma.db]
        [twitter.api-client]
        [amqp.message-handler])
  (:gen-class))

(defn -main
  "Command dispatch application (AMQP message consumption / recommendation)"
  [name & args]
  (cond
    (= name "consume-amqp-message")
      (let [[queue messages consumers] args
            total-messages (if (nil? messages)
                             100
                             (Long/parseLong messages))
            parallel-consumers (if (nil? consumers)
                                 1
                                 (Long/parseLong consumers))]
          (consume-messages (keyword queue) total-messages parallel-consumers))
    (= name "recommend-subscriptions")
      (let [[screen-name] args]
          (recommand-subscriptions-for-member-having-screen-name screen-name))))
