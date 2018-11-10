(ns amqp.message-handler
    (:require [clojure.data.json :as json]
              [clojure.edn :as edn]
              [clojure.spec.alpha :as s]
              [environ.core :refer [env]]
              [langohr.channel :as lch]
              [langohr.basic :as lb]
              [langohr.consumers :as lc]
              [langohr.core :as rmq]
              [clj-time.core :as t]
              [clj-time.local :as l]
              [clj-time.format :as f]
              [clj-time.coerce :as c]
              [clojure.tools.logging :as log]
              [php_clj.core :refer [php->clj clj->php]])
    (:use [repository.entity-manager]
          [twitter.api-client])
    (:import java.util.Locale))

(defn new-member-from-json
  [member-id tokens members]
  (log/info (str "About to look up for member having twitter id #" member-id))
  (let [twitter-user (get-member-by-id member-id tokens members)
        member (new-member {:description (:description twitter-user)
                            :is-protected (if (not= (:protected twitter-user) "false") 1 0)
                            :is-suspended 0
                            :is-not-found 0
                            :total-subscribees (:followers_count twitter-user)
                            :total-subscriptions (:friends_count twitter-user)
                            :twitter-id (:id_str twitter-user)
                            :screen-name (:screen_name twitter-user)} members)]
    member))

(defn ensure-members-exist
  [members-ids tokens members]
  (let [remaining-calls (how-many-remaining-calls-showing-user tokens)
        total-members (count members-ids)]

  (if (pos? total-members)
    (log/info (str "About to ensure " total-members " member(s) exist."))
    (log/info (str "No need to find some missing member.")))

  (if (and
        (not (nil? remaining-calls))
        (< total-members remaining-calls))
    (doall (pmap #(:id (new-member-from-json % tokens members)) members-ids))
    (doall (map #(:id (new-member-from-json % tokens members)) members-ids)))))

(defn process-subscriptions
  [member-id screen-name member-subscription-model token-model member-model]
  (let [subscriptions-ids (get-subscriptions-of-member screen-name token-model)
        matching-subscriptions-members (find-members-by-id subscriptions-ids member-model)
        matching-subscriptions-members-ids (map-get-in :id matching-subscriptions-members)
        missing-subscriptions-members-ids (deduce-ids-of-missing-members matching-subscriptions-members subscriptions-ids)]

    (if missing-subscriptions-members-ids
      (ensure-subscriptions-exist-for-member-having-id {:member-id member-id
                                                        :model member-subscription-model
                                                        :matching-subscriptions-members-ids
                                                                   (ensure-members-exist missing-subscriptions-members-ids token-model member-model)})
      (log/info (str "No member missing from subscriptions of member \"" screen-name "\"")))
    (ensure-subscriptions-exist-for-member-having-id {:member-id member-id
                                                      :model member-subscription-model
                                                      :matching-subscriptions-members-ids matching-subscriptions-members-ids})))

(defn process-subscribees
  [member-id screen-name member-subscribee-model token-model member-model]
  (let [subscribees-ids (get-subscribees-of-member screen-name token-model)
        matching-subscribees-members (find-members-by-id subscribees-ids member-model)
        matching-subscribees-members-ids (map-get-in :id matching-subscribees-members)
        missing-subscribees-members-ids (deduce-ids-of-missing-members matching-subscribees-members subscribees-ids)]

    (if missing-subscribees-members-ids
      (ensure-subscribees-exist-for-member-having-id {:member-id member-id
                                                      :model member-subscribee-model
                                                      :matching-subscribees-members-ids (ensure-members-exist missing-subscribees-members-ids tokens members)})
      (log/info (str "No member missing from subscribees of member \"" screen-name "\"")))
    (ensure-subscribees-exist-for-member-having-id {:member-id member-id
                                                    :model member-subscribee-model
                                                    :matching-subscribees-members-ids matching-subscribees-members-ids})))

(defn process-network
  [payload entity-manager]
  (let [{members :members
         tokens :tokens
         member-subscriptions :member-subscriptions
         member-subscribees :member-subscribees} entity-manager
        screen-name (first (json/read-str (php->clj (String. payload  "UTF-8"))))
        member-id (get-id-of-member-having-username screen-name members tokens)]
    (process-subscriptions member-id screen-name member-subscriptions tokens members)
    (process-subscribees member-id screen-name member-subscribees tokens members)))

(defn get-time-range
  [timestamp]
  (let [now (c/to-long (l/local-now))
        date (c/from-long timestamp)
        five-minutes-ago (c/from-long (- now (* 60 5 1000)))
        ten-minutes-ago (c/from-long (- now (* 60 10 1000)))
        thirty-minutes-ago (c/from-long (- now (* 60 30 1000)))
        one-day-ago (c/from-long (- now (* 3600 24 1000)))
        one-week-ago (c/from-long (- now (* 7 3600 24 1000)))]
        (cond
          (t/after? date five-minutes-ago)
            0
          (t/after? date ten-minutes-ago)
            1
          (t/after? date thirty-minutes-ago)
            2
          (t/after? date one-day-ago)
            3
          (t/after? date one-week-ago)
            4
          :else
            5)))

(def date-formatter (f/with-locale (f/formatter "EEE MMM dd HH:mm:ss Z yyyy") Locale/ENGLISH))
(def mysql-date-formatter (f/formatters :mysql))

(defn process-like
  [like liked-by aggregate model status-model member-model token-model]
  (let [{twitter-id :id_str
         created-at :created_at
         text :full_text
         {screen-name :screen_name
          user-id :id
          avatar :profile_image_url
          name :name} :user } like
        token (:token @next-token)
        document (json/write-str like)
        parsed-publication-date (c/to-long (f/parse date-formatter created-at))
        _ (ensure-members-exist (list user-id) token-model member-model)
        liked-member (find-member-by-twitter-id user-id member-model)
        existing-liked-status (find-status-by-twitter-id twitter-id status-model)
        status (if (pos? (count existing-liked-status))
                 (first existing-liked-status)
                 (new-status {:text text
                              :screen-name screen-name
                              :avatar avatar
                              :name name
                              :token token
                              :document document
                              :created-at (f/unparse mysql-date-formatter (c/from-long parsed-publication-date))
                              :twitter-id twitter-id} status-model))
        liked-member-id (:id liked-member)
        liked-by-member-id (:id liked-by)
        status-id (:id status)
        existing-liked-status (find-liked-status-by liked-member-id liked-by-member-id status-id model)
        liked-status (if existing-liked-status
                       existing-liked-status
                       (new-liked-status {:aggregate-id (:id aggregate)
                           :aggregate-name (:name aggregate)
                           :time-range (get-time-range parsed-publication-date)
                           :status_id status-id
                           :is-archived-status 0
                           :member-id liked-member-id
                           :member-name (:screen-name liked-member)
                           :liked-by liked-by-member-id
                           :liked-by-member-name (:screen-name liked-by)}
                          model))]
      liked-status))

(defn process-likes
  [payload entity-manager]
  (let [{member-model :members
         token-model :tokens
         aggregate-model :aggregates
         liked-status-model :liked-status
         status-model :status} entity-manager
        payload-body (json/read-str (php->clj (String. payload  "UTF-8")))
        screen-name (get payload-body "screen_name")
        aggregate-id (get payload-body "aggregate_id")
        aggregate (find-aggregate-by-id aggregate-id aggregate-model)
        member (first (find-member-by-screen-name screen-name member-model))
        favorites (get-favorites-of-member screen-name token-model)]
    (doall (map #(process-like %
                               member
                               aggregate
                               liked-status-model
                               status-model
                               member-model
                               token-model) favorites))))

(defn get-message-handler
  "Get AMQP message handler"
  ; About RabbitMQ message consumption and Clojure
  ; @see http://clojurerabbitmq.info/articles/getting_started.html#hello-world-example
  [entity-manager]
  (fn
      [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
      (process-network payload entity-manager)
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
        entity-manager (get-entity-manager (:database environment-configuration))
        message-handler (get-message-handler entity-manager)]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    {:channel ch
     :message-handler message-handler
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
        [{:keys [delivery-tag]} payload] (lb/get channel queue auto-ack)]
    (process-likes payload entity-manager)
    ;(lb/ack channel delivery-tag)
    ))

(s/def ::total-messages #(pos-int? %))

(defn consume-messages
  [total-messages queue]
  {:pre [(s/valid? ::total-messages total-messages)]}
  (let [rabbitmq (edn/read-string (:rabbitmq env))
        {connection :connection
         channel :channel
         message-handler :message-handler} (connect-to-amqp-server env)]
    (if total-messages
      (loop [messages-to-consume total-messages]
        (when (> messages-to-consume 0)
          (cond
            (= queue :network)
              (pull-messages-from-network-queue {:auto-ack false
                                                 :entity-manager (get-entity-manager (:database env))
                                                 :total-messages total-messages
                                                 :queue (:queue-network rabbitmq)
                                                 :channel channel
                                                 :message-handler message-handler})
            (= queue :likes)
              (pull-messages-from-likes-queue {:auto-ack false
                                               :entity-manager (get-entity-manager (:database env))
                                               :total-messages total-messages
                                               :queue (:queue-likes rabbitmq)
                                               :channel channel
                                               :message-handler message-handler}))
          (recur (dec messages-to-consume))))
      (lc/subscribe channel queue message-handler {:auto-ack false}))
  (Thread/sleep 1000)
  (disconnect-from-amqp-server channel connection)))