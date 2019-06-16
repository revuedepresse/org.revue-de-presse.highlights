(ns amqp.list-handler
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [php_clj.core :refer [php->clj clj->php]])
  (:use [amqp.handling-errors]
        [command.collect-timely-statuses]))

(defn wait-for-new-messages
  [channel queue auto-ack]
  (let [message (atom [])
        _ (swap! message (constantly (lb/get channel queue auto-ack)))]
    (loop [wait-for-15-more-seconds (nil? (second @message))]
      (when wait-for-15-more-seconds
        (log/info (str "About to wait for 15 seconds for new messages"))
        (Thread/sleep (* 15 1000))
        (swap! message (constantly (lb/get channel queue auto-ack)))
        (recur (nil? (second @message)))))
    @message))

(defn pull-messages-from-lists-queue
  "Pull messages from a queue dedicated to status collection from lists"
  [options]
  (let [{auto-ack :auto-ack
         channel :channel
         queue :queue
         entity-manager :entity-manager} options
        new-message (wait-for-new-messages channel queue auto-ack)
        {:keys [delivery-tag]} (first new-message)
        payload (second new-message)
        payload-body (json/read-str (php->clj (String. payload "UTF-8")))
        screen-name (get payload-body "screen_name")
        aggregate-id (get payload-body "aggregate_id")]
    (when payload
      (try
        (do
          (handle-list
            {:screen-name                   screen-name
             :aggregate-id                  aggregate-id
             :entity-manager                entity-manager
             :unavailable-aggregate-message error-unavailable-aggregate})
          (lb/ack channel delivery-tag))
        (catch Exception e
          (log/error "An error occurred with message " (.getMessage e)))))))

(defn pull-messages-from-status-queue
  "Pull messages from a queue dedicated to status collection"
  [options]
  (let [{auto-ack       :auto-ack
         channel        :channel
         queue          :queue
         entity-manager :entity-manager} options
        new-message (wait-for-new-messages channel queue auto-ack)
        {:keys [delivery-tag]} (first new-message)
        payload (second new-message)
        payload-body (json/read-str (php->clj (String. payload "UTF-8")))
        screen-name (get payload-body "screen_name")
        aggregate-id (get payload-body "aggregate_id")]
    (when payload
      (try
        (do
          (handle-list
            {:screen-name                   screen-name
             :aggregate-id                  aggregate-id
             :entity-manager                entity-manager
             :unavailable-aggregate-message error-unavailable-aggregate})
          (lb/ack channel delivery-tag))
        (catch Exception e
          (log/error "An error occurred with message " (.getMessage e)))))))
