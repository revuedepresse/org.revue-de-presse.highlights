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

(defn assoc-twitter-user-properties
  [twitter-user]
  {:description (:description twitter-user)
   :is-protected (if (not= (:protected twitter-user) "false") 1 0)
   :is-suspended 0
   :is-not-found 0
   :total-subscribees (:followers_count twitter-user)
   :total-subscriptions (:friends_count twitter-user)
   :twitter-id (:id_str twitter-user)
   :screen-name (:screen_name twitter-user)})

(defn assoc-properties-of-twitter-users
  [twitter-users]
  (log/info "Build a sequence of twitter users properties")
  (doall (pmap assoc-twitter-user-properties twitter-users)))

(defn get-twitter-user
  [member-id tokens members]
  (log/info (str "About to look up for member having twitter id #" member-id))
  (let [twitter-user (get-member-by-id member-id tokens members)]
    twitter-user))

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
        matching-subscriptions-members (find-members-having-ids subscriptions-ids member-model)
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
        matching-subscribees-members (find-members-having-ids subscribees-ids member-model)
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

(defn ensure-member-having-id-exists
  [twitter-id model token-model]
  (let [existing-member (find-member-by-twitter-id twitter-id model)
        member (if (pos? (count existing-member))
                     (first existing-member)
                     (do
                       (ensure-members-exist (list twitter-id) token-model model)
                       (first (find-member-by-twitter-id twitter-id model))))]
    member))

(defn ensure-status-having-id-exists
  [{twitter-id :id_str
    text :full_text
    created-at :created_at
    {screen-name :screen_name
     avatar :profile_image_url
     name :name} :user
    :as status}
   model]
  (let [document (json/write-str status)
        parsed-publication-date (c/to-long (f/parse date-formatter created-at))
        mysql-formatted-publication-date (f/unparse mysql-date-formatter (c/from-long parsed-publication-date))
        existing-liked-status (find-status-by-twitter-id twitter-id model)
        token (:token @next-token)
        status (if (pos? (count existing-liked-status))
           (first existing-liked-status)
           (new-status {:text text
                        :screen-name screen-name
                        :avatar avatar
                        :name name
                        :token token
                        :document document
                        :created-at mysql-formatted-publication-date
                        :twitter-id twitter-id} model))]
    status))

(defn ensure-favorited-status-exists
  [aggregate favorite favorite-author model status-model member-model token-model]
  (let [{created-at :created_at
         {liked-twitter-user-id :id} :user} favorite
        liked-member (ensure-member-having-id-exists liked-twitter-user-id member-model token-model)
        status (ensure-status-having-id-exists favorite status-model)
        parsed-publication-date (c/to-long (f/parse date-formatter created-at))
        mysql-formatted-publication-date (f/unparse mysql-date-formatter (c/from-long parsed-publication-date))
        liked-member-id (:id liked-member)
        liked-by-member-id (:id favorite-author)
        status-id (:id status)
        existing-liked-status (find-liked-status-by liked-member-id liked-by-member-id status-id model status-model)
        favorited-status (if (pos? (count existing-liked-status))
                           (first existing-liked-status)
                           {:id nil
                            :aggregate-id (:id aggregate)
                            :aggregate-name (:name aggregate)
                            :time-range (get-time-range parsed-publication-date)
                            :publication-date-time mysql-formatted-publication-date
                            :status-id status-id
                            :is-archived-status 0
                            :member-id liked-member-id
                            :member-name (:screen-name liked-member)
                            :liked-by liked-by-member-id
                            :liked-by-member-name (:screen-name favorite-author)})]
    {:favorite favorited-status
     :favorite-author favorite-author
     :status status
     :status-author liked-member}))

(defn process-like
  [like liked-by aggregate model status-model member-model token-model]
  (let [favorited-status (ensure-favorited-status-exists aggregate
                                                         like
                                                         liked-by
                                                         model
                                                         status-model
                                                         member-model
                                                         token-model)]
      favorited-status))

(defn get-ids-of-statuses-authors
  [statuses]
  (map (fn [{{member-id :id_str} :user}] member-id) statuses))

(defn get-missing-members-ids
  [statuses model]
  (let [ids (get-ids-of-statuses-authors statuses)
        found-members (find-members-having-ids ids model)
        matching-ids (set (map #(:twitter-id %) found-members))
        missing-ids (clojure.set/difference (set ids) (set matching-ids))]
    missing-ids))

(defn ensure-authors-of-favorited-status-exist
  [statuses model token-model]
  (let [remaining-calls (how-many-remaining-calls-showing-user token-model)
        authors-ids (map #(:id (:user %)) statuses)
        total-authors (count authors-ids)]

    (if (pos? total-authors)
      (do
        (log/info (str "About to ensure " total-authors " member(s) exist."))
        (let [twitter-users (if
                              (and
                                (not (nil? remaining-calls))
                                (< total-authors remaining-calls))
                              (pmap #(get-twitter-user % token-model model) authors-ids)
                              (map #(get-twitter-user % token-model model) authors-ids))
              twitter-users-properties (assoc-properties-of-twitter-users twitter-users)
              deduplicated-users-properties (dedupe (sort-by #(:twitter-id %) twitter-users-properties))
              new-members (bulk-insert-new-members deduplicated-users-properties model)]
          (doall (map #(log/info (str "Member #" (:twitter-id %)
                                   " having screen name \"" (:screen-name %)
                                   "\" has been saved under id \"" (:id %))) new-members)))
      (log/info (str "No need to find some missing member."))))))

(defn process-authors-of-favorited-status
  [favorites model token-model]
  (let [missing-members-ids (get-missing-members-ids favorites model)
        missing-members (filter #(clojure.set/subset? #{(:id_str (:user %))} missing-members-ids) favorites)
        _ (ensure-authors-of-favorited-status-exist missing-members model token-model)]))

(defn get-ids-of-statuses
  [statuses]
  (map (fn [{twitter-id :id_str}] twitter-id) statuses))

(defn get-missing-statuses-ids
  [statuses model]
  (let [ids (get-ids-of-statuses statuses)
        found-statuses (find-statuses-having-ids ids model)
        matching-ids (set (map #(:twitter-id %) found-statuses))
        missing-ids (clojure.set/difference (set ids) (set matching-ids))]
    missing-ids))

(defn get-status-json
  [{twitter-id :id_str
    text :full_text
    created-at :created_at
    {screen-name :screen_name
     avatar :profile_image_url
     name :name} :user
    :as status}]
  (let [document (json/write-str status)
        token (:token @next-token)
        parsed-publication-date (c/to-long (f/parse date-formatter created-at))
        mysql-formatted-publication-date (f/unparse mysql-date-formatter (c/from-long parsed-publication-date))
        twitter-status {:text text
                        :full-name screen-name
                        :avatar avatar
                        :name name
                        :access-token token
                        :api-document document
                        :created-at mysql-formatted-publication-date
                        :status-id twitter-id}]
    twitter-status))

(defn ensure-statuses-exist
  [statuses model]
  (let [total-statuses (count statuses)
        new-statuses (if
                       (pos? total-statuses)
                        (do
                          (log/info (str "About to ensure " total-statuses " statuses exist."))
                          (bulk-insert-new-statuses
                            (pmap #(get-status-json %) statuses)
                            model))
                        (do
                          (log/info (str "No need to find some missing status."))
                          '()))]
    (when (pos? total-statuses)
      (doall (map #(log/info (str "Status #" (:twitter-id %)
                                  " authored by \"" (:screen-name %)
                                  "\" has been saved under id \"" (:id %))) new-statuses)))
    new-statuses))

(defn process-favorited-statuses
  [favorites model]
  (let [missing-statuses-ids (get-missing-statuses-ids favorites model)
        remaining-favorites (filter #(clojure.set/subset? #{(:id_str %)} missing-statuses-ids) favorites)]
    (when (pos? (count missing-statuses-ids))
      (ensure-statuses-exist remaining-favorites model))))

(defn preprocess-favorites
  [favorites status-model member-model token-model]
  (when (pos? (count favorites))
    (process-favorited-statuses favorites status-model)
    (process-authors-of-favorited-status favorites member-model token-model)))

(defn get-next-batch-of-favorites-for-member
  [member token-model]
  (let [screen-name (:screen-name member)
        max-favorite-id (:max-favorite-status-id member)
        min-favorite-id (:min-favorite-status-id member)
        _ (if max-favorite-id
            (log/info (str "About to fetch favorites since status #" max-favorite-id))
            (log/info (str "About to fetch favorites until reaching status #" min-favorite-id)))
        favorites (if max-favorite-id
                    (get-favorites-of-member
                      {:screen-name screen-name
                       :since-id (inc (Long/parseLong (:max-favorite-status-id member)))}
                      token-model)
                    (get-favorites-of-member
                      {:screen-name screen-name
                       :max-id (if (nil? (:min-favorite-status-id member))
                                 nil
                                 (dec (Long/parseLong (:min-favorite-status-id member))))}
                      token-model))
        next-batch-of-favorites (when (and
                                        max-favorite-id
                                        (= (count favorites) 0))
                                          (get-favorites-of-member
                                            {:screen-name screen-name
                                             :max-id (if (nil? (:min-favorite-status-id member))
                                                       nil
                                                       (dec (Long/parseLong (:min-favorite-status-id member))))}
                                            token-model))]
    (if (pos? (count favorites))
      favorites
      next-batch-of-favorites)))

(defn process-member-favorites
  [member favorites aggregate {liked-status-model :liked-status
                               member-model :members
                               status-model :status
                               token-model :tokens}]
  (let [ _ (preprocess-favorites favorites status-model member-model token-model)
        processed-likes (pmap #(process-like %
                                             member
                                             aggregate
                                             liked-status-model
                                             status-model
                                             member-model
                                             token-model) favorites)
        missing-favorites-statuses (filter #(nil? (:id (:favorite %))) processed-likes)
        new-favorites (new-liked-statuses (map
                                            #(:favorite %)
                                            missing-favorites-statuses)
                                          liked-status-model
                                          status-model)]
    (doall (map #(log/info
                   (str
                     "Status #" (:status-id %)
                     " published by \"" (:member-name %)
                     "\" and liked by \"" (:liked-by-member-name %)
                     "\" has been saved under id \"" (:id %) "\"")) new-favorites))
    processed-likes))

(defn update-max-favorite-id-for-member
  [member aggregate entity-manager]
  (let [screen-name (:screen-name member)
        member-model (:members entity-manager)
        token-model (:tokens entity-manager)
        member-id (:id member)
        latest-favorites (get-favorites-of-member {:screen-name screen-name} token-model)
        _ (process-member-favorites member latest-favorites aggregate entity-manager)
        latest-status-id (:id_str (first latest-favorites))]
    (update-max-favorite-id-for-member-having-id latest-status-id member-id member-model)))

(defn process-likes
  [payload entity-manager]
  (let [{member-model :members
         token-model :tokens
         aggregate-model :aggregates} entity-manager
        payload-body (json/read-str (php->clj (String. payload  "UTF-8")))
        screen-name (get payload-body "screen_name")
        aggregate-id (get payload-body "aggregate_id")
        aggregate (first (find-aggregate-by-id aggregate-id aggregate-model))
        member (first (find-member-by-screen-name screen-name member-model))
        favorites (get-next-batch-of-favorites-for-member member token-model)
        processed-likes (process-member-favorites member favorites aggregate entity-manager)
        last-favorited-status (last processed-likes)
        status (:status last-favorited-status)
        favorite-author (:favorite-author last-favorited-status)]

        (when
          (and
            (not (nil? status))
            (not (nil? favorite-author)))
          (update-min-favorite-id-for-member-having-id (:twitter-id status)
                                                       (:id favorite-author)
                                                       member-model))
        (if (pos? (count processed-likes))
          (process-likes payload entity-manager)
          (update-max-favorite-id-for-member member aggregate entity-manager))))

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
        [{:keys [delivery-tag]} payload] (lb/get channel queue auto-ack)]
    (when payload
      (process-likes payload entity-manager)
      (lb/ack channel delivery-tag))))

(s/def ::total-messages #(pos-int? %))

(defn consume-message
  [entity-manager rabbitmq channel queue & [message-index]]
  (cond
    (= queue :network)
      (pull-messages-from-network-queue {:auto-ack false
                                         :entity-manager entity-manager
                                         :queue (:queue-network rabbitmq)
                                         :channel channel})
    (= queue :likes)
      (pull-messages-from-likes-queue {:auto-ack false
                                       :entity-manager entity-manager
                                       :queue (:queue-likes rabbitmq)
                                       :channel channel}))
  (when message-index
    (log/info (str "Consumed message #" message-index))))

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
      (loop [messages-to-consume total-messages]
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