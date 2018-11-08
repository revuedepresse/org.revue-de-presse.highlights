(ns amqp.message-handler
    (:require [clojure.data.json :as json]
              [clojure.edn :as edn]
              [clojure.spec.alpha :as s]
              [environ.core :refer [env]]
              [langohr.channel :as lch]
              [langohr.basic :as lb]
              [langohr.consumers :as lc]
              [langohr.core :as rmq]
              [clojure.tools.logging :as log]
              [php_clj.core :refer [php->clj clj->php]])
    (:use [repository.entity-manager]
          [twitter.api-client]))

(def ^:dynamic *skip-subscriptions* false)

(def ^:dynamic *skip-subscribees* false)

(defn new-member-from-json
  [member-id tokens members]
  (let [twitter-user (get-member-by-id member-id tokens members)
        member (new-member {:description (:description twitter-user)
                :is-protected (if (not= (:protected twitter-user) "false") 1 0)
                :is-suspended 0
                :is-not-found 01
                :total-subscribees (:followers_count twitter-user)
                :total-subscriptions (:friends_count twitter-user)
                :twitter-id (:id_str twitter-user)
                :screen-name (:screen_name twitter-user)} members)]
    member))

(defn ensure-members-exist
  [members-ids tokens members]
  (log/info (str "About to ensure " (count members-ids) " member(s) exist."))
  (doall (map #(:id (new-member-from-json % tokens members)) members-ids)))

(defn process-payload
  [payload entity-manager]
  (let [{members :members
         tokens :tokens
         member-subscriptions :member-subscriptions
         member-subscribees :member-subscribees} entity-manager
        screen-name (first (json/read-str (php->clj (String. payload  "UTF-8"))))
        member-id (get-id-of-member-having-username screen-name members tokens)
        subscribees-ids (get-subscribees-of-member screen-name tokens)
        matching-subscribees-members (find-members-by-id subscribees-ids members)
        matching-subscribees-members-ids (map-get-in :id matching-subscribees-members)
        missing-subscribees-members-ids (deduce-ids-of-missing-members matching-subscribees-members subscribees-ids)
        subscriptions-ids (get-subscriptions-of-member screen-name tokens)
        matching-subscriptions-members (find-members-by-id subscriptions-ids members)
        matching-subscriptions-members-ids (map-get-in :id matching-subscriptions-members)
        missing-subscriptions-members-ids (deduce-ids-of-missing-members matching-subscriptions-members subscriptions-ids)]

    (when (not *skip-subscriptions*)
      (if missing-subscriptions-members-ids
        (ensure-subscriptions-exist-for-member-having-id {:member-id member-id
                                                          :model member-subscriptions
                                                          :matching-subscriptions-members-ids
                                                                     (ensure-members-exist missing-subscriptions-members-ids tokens members)})
        (log/info (str "No member missing from subscriptions of member \"" screen-name "\"")))
        (ensure-subscriptions-exist-for-member-having-id {:member-id member-id
                                                         :model member-subscriptions
                                                         :matching-subscriptions-members-ids matching-subscriptions-members-ids}))

    (when (not *skip-subscribees*)
      (if missing-subscribees-members-ids
        (ensure-subscribees-exist-for-member-having-id {:member-id member-id
                                                        :model member-subscribees
                                                        :matching-subscribees-members-ids (ensure-members-exist missing-subscribees-members-ids tokens members)})
        (log/info (str "No member missing from subscribees of member \"" screen-name "\"")))
        (ensure-subscribees-exist-for-member-having-id {:member-id member-id
                                                       :model member-subscribees
                                                       :matching-subscribees-members-ids matching-subscribees-members-ids}))))

(defn get-message-handler
  "Get AMQP message handler"
  ; About RabbitMQ message consumption and Clojure
  ; @see http://clojurerabbitmq.info/articles/getting_started.html#hello-world-example
  [entity-manager]
  (fn
      [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
      (process-payload payload entity-manager)
      (lb/ack ch delivery-tag)))

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
        [{:keys [delivery-tag]} payload] (lb/get channel queue auto-ack)]
    (process-payload payload entity-manager)
    (lb/ack channel delivery-tag)))

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
      (loop [messages-to-consume total-messages]
        (when (> messages-to-consume 0)
          (pull-messages-from-queue {:auto-ack false
                                   :entity-manager (get-entity-manager (:database env))
                                   :total-messages total-messages
                                   :queue queue
                                   :channel channel
                                   :message-handler message-handler})
          (recur (dec messages-to-consume))))
      (lc/subscribe channel queue message-handler {:auto-ack false}))
  (Thread/sleep 1000)
  (disconnect-from-amqp-server channel connection)))