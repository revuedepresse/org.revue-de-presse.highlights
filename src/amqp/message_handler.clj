(ns amqp.message-handler
    (:require [clojure.data.json :as json]
              [clojure.edn :as edn]
              [clojure.spec.alpha :as s]
              [environ.core :refer [env]]
              [langohr.channel :as lch]
              [langohr.basic :as lb]
              [langohr.core :as rmq]
              [clojure.tools.logging :as log]
              [php_clj.core :refer [php->clj clj->php]])
    (:use [twitter.favorited-status]
          [repository.entity-manager]
          [twitter.status]
          [twitter.member]
          [amqp.favorite-status-handler]
          [amqp.list-handler]
          [amqp.network-handler]
          [twitter.api-client]))

(defn disconnect-from-amqp-server
  "Close connection and channel"
  [ch conn]
  (println "[main] Disconnecting...")
  (rmq/close ch)
  (rmq/close conn))

(defn connect-to-amqp-server
  [environment-configuration]
  (let [rabbitmq (edn/read-string (:rabbitmq environment-configuration))
        conn (rmq/connect {:host (:host rabbitmq)
                           :username (:user rabbitmq)
                           :password (:password rabbitmq)
                           :port (Integer/parseInt (:port rabbitmq))
                           :vhost (:vhost rabbitmq)})
        ch (lch/open conn)]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    {:channel ch
     :connection conn}))

(defn pull-messages-from-network-queue
  "Pull messages from a queue dedicated to network discovery"
  [options]
  (let [{auto-ack :auto-ack
         channel :channel
         queue :queue
         entity-manager :entity-manager} options
        [{:keys [delivery-tag]} payload] (lb/get channel queue auto-ack)]
    (process-network payload entity-manager)
    (lb/ack channel delivery-tag)))

(defn pull-messages-from-likes-queue
  "Pull messages from a queue dedicated to likes collection"
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
          (process-likes screen-name aggregate-id entity-manager)
          (lb/ack channel delivery-tag))
        (catch Exception e
          (cond
            (= (.getMessage e) error-mismatching-favorites-cols-length)
              (log/error "Likes of \"" screen-name "\" related to aggregate #"
                         aggregate-id " could not be processed")
            (= (.getMessage e) error-unavailable-aggregate)
              (do
                (log/error "Likes of \"" screen-name "\" related to aggregate #"
                         aggregate-id " could not be bound to an actual aggregate")
                (lb/ack channel delivery-tag))
            :else
              (log/error "An error occurred with message " (.getMessage e))))))))

(defn consume-message
  [entity-manager rabbitmq channel queue & [message-index]]
  (cond
    (= queue :likes)
      (pull-messages-from-likes-queue {:auto-ack false
                                     :entity-manager entity-manager
                                     :queue (:queue-likes rabbitmq)
                                     :channel channel})
    (= queue :lists)
      (pull-messages-from-lists-queue {:auto-ack false
                                     :entity-manager entity-manager
                                     :queue (:queue-lists rabbitmq)
                                     :channel channel})
    (= queue :network)
      (pull-messages-from-network-queue {:auto-ack false
                                         :entity-manager entity-manager
                                         :queue (:queue-network rabbitmq)
                                         :channel channel})
    :else
      (println (str "Unknown queue name, please select one of the following: lists, likes or network")))
  (when message-index
    (log/info (str "Consumed message #" message-index))))

(s/def ::total-messages #(pos-int? %))

(defn consume-messages
  [queue total-messages parallel-consumers]
  {:pre [(s/valid? ::total-messages total-messages)]}
  (let [entity-manager (get-entity-manager (:database env))
        rabbitmq (edn/read-string (:rabbitmq env))
        {connection :connection
         channel :channel} (connect-to-amqp-server env)
        single-message-consumption (= 1 parallel-consumers)
        next-total (if single-message-consumption #(dec %) #(max 0 (- % parallel-consumers)))]
    (log/info (str "About to consume " total-messages " messages."))
    (if total-messages
      (loop [messages-to-consume (if total-messages total-messages 0)]
        (when (> messages-to-consume 0)
          (if single-message-consumption
            (consume-message entity-manager rabbitmq channel queue))
            (doall
              (pmap
                #(consume-message entity-manager rabbitmq channel queue %)
                (take total-messages (iterate inc 1))))
          (recur (next-total messages-to-consume)))))
  (Thread/sleep 1000)
  (disconnect-from-amqp-server channel connection)))
