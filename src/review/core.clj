; About gen-class examples
; @see https://clojure.org/reference/compilation#_gen_class_examples
(ns review.core
  (:use [korma.db]
        [twitter.api-client]
        [amqp.message-handler])
  (:gen-class))

(defn -main
  "AMQP message consuming application"
  [queue & [messages consumers]]
  (let [total-messages (if (nil? messages)
                         100
                         (Long/parseLong messages))
        parallel-consumers (if (nil? consumers)
                             1
                             (Long/parseLong consumers))]
      (consume-messages (keyword queue) total-messages parallel-consumers)))
