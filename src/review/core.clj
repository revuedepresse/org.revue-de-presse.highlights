; About gen-class examples
; @see https://clojure.org/reference/compilation#_gen_class_examples
(ns review.core
  (:use [korma.db]
        [twitter.api-client]
        [amqp.message-handler])
  (:gen-class))

(defn -main
  "AMQP message consuming application"
  ([queue & [messages]]
   (if (nil? messages)
     (consume-messages (keyword queue) 100)
     (consume-messages (keyword queue) (Long/parseLong messages)))))
