(ns amqp.message-handler
    (:require [clojure.data.json :as json]
              [clojure.edn :as edn]
              [environ.core :refer [env]]
              [langohr.channel :as lch]
              [langohr.consumers :as lc]
              [langohr.core :as rmq]
              [php_clj.core :refer [php->clj clj->php]])
    (:use [repository.entity-manager]
          [twitter.api-client]))

(defn get-message-handler
  "Get AMQP message handler"
  ; About RabbitMQ message consumption and Clojure
  ; @see http://clojurerabbitmq.info/articles/getting_started.html#hello-world-example
  [users]
  (fn
    [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
    (let [member (find-user-by-username (first (json/read-str (php->clj (String. payload  "UTF-8")))) users)
          screen-name (:screen_name (first member))]
          (println (get-member-by-screen-name screen-name)))))

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
        entities (connect-to-db (edn/read-string (:database environment-configuration)))
        message-handler (get-message-handler (:users entities))]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    {:channel ch
     :queue qname
     :message-handler message-handler
     :connection conn}))

(defn consume-network-queue
  []
  (let [{connection :connection
         channel :channel
         queue :queue
         message-handler :message-handler} (connect-to-amqp-server env)]
  (lc/subscribe channel queue message-handler {:auto-ack false})
  (Thread/sleep 60000)
  (disconnect-from-amqp-server channel connection)))