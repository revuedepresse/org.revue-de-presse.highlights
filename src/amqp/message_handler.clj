(ns amqp.message-handler
    (:require [clojure.data.json :as json]
              [clojure.edn :as edn]
              [clojure.spec.alpha :as s]
              [environ.core :refer [env]]
              [langohr.channel :as lch]
              [langohr.basic :as lb]
              [langohr.consumers :as lc]
              [langohr.core :as rmq]
              [php_clj.core :refer [php->clj clj->php]])
    (:use [repository.entity-manager]
          [twitter.api-client]))

(defn process-payload
  [payload entity-manager]
  (let [{members              :members
         member-subscriptions :member-subscriptions} entity-manager
        screen-name "gpeal8"                                ;(first (json/read-str (php->clj (String. payload  "UTF-8"))))
        member-id (get-id-of-member-having-username screen-name members)
        subscriptions-ids (get-subscriptions-of-member screen-name)
        matching-subscriptions-members-ids (find-members-ids-by-id subscriptions-ids members)]

    ;(println ((ensure-subscriptions-exists-for-member-having-id member-id member-subscriptions)
    ;           first-match)))

    (ensure-subscriptions-exists-for-member-having-id {:member-id member-id
                                                       :member-subscriptions member-subscriptions
                                                       :matching-subscriptions-members-ids matching-subscriptions-members-ids})))
    ;(println (get-subscribees-of-member screen-name))

(defn get-message-handler
  "Get AMQP message handler"
  ; About RabbitMQ message consumption and Clojure
  ; @see http://clojurerabbitmq.info/articles/getting_started.html#hello-world-example
  [entity-manager]
  (fn
      [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
      process-payload payload entity-manager))

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
        ch (lch/open conn)
        qname (:queue rabbitmq)
        entity-manager (get-entity-manager (:database environment-configuration))
        message-handler (get-message-handler entity-manager)]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    {:channel ch
     :queue qname
     :message-handler message-handler
     :connection conn}))

(defn pull-messages-from-queue
  "Pull messages from a queue"
  [options]
  (let [{auto-ack :auto-ack
         channel :channel
         queue :queue
         entity-manager :entity-manager
         message-handler :message-handler
         total-messages :total-messages} options
        [metadata payload] (lb/get channel queue auto-ack)]
    (process-payload payload entity-manager)))

(s/def ::total-messages #(or (nil? %) (pos-int? %)))

(defn consume-messages-from-network-queue
  [& args]
  {:pre [(s/valid? ::total-messages (first args))]}
  (let [total-messages (first args)
        {connection :connection
         channel :channel
         queue :queue
         message-handler :message-handler} (connect-to-amqp-server env)]
    (if total-messages
      (pull-messages-from-queue {:auto-ack false
                                 :entity-manager (get-entity-manager (:database env))
                                 :total-messages total-messages
                                 :queue queue
                                 :channel channel
                                 :message-handler message-handler})
      (lc/subscribe channel queue message-handler {:auto-ack false}))
  (Thread/sleep 60000)
  (disconnect-from-amqp-server channel connection)))