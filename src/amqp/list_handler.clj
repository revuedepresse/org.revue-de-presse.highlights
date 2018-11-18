(ns amqp.list-handler
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [php_clj.core :refer [php->clj clj->php]])
  (:use [amqp.handling-errors]
        [amqp.status-handler]))

(defn pull-messages-from-lists-queue
  "Pull messages from a queue dedicated to status collection from lists"
  [options]
  (let [{auto-ack       :auto-ack
         channel        :channel
         queue          :queue
         entity-manager :entity-manager} options
        [{:keys [delivery-tag]} payload] (lb/get channel queue auto-ack)
        payload-body (json/read-str (php->clj (String. payload "UTF-8")))
        screen-name (get payload-body "screen_name")
        aggregate-id (get payload-body "aggregate_id")]
    (when payload
      (try
        (do
          (process-lists screen-name aggregate-id entity-manager error-unavailable-aggregate)
          ;(lb/ack channel delivery-tag)
          )
        (catch Exception e
          (log/error "An error occurred with message " (.getMessage e)))))))
