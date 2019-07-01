(ns repository.member-subscription
  (:require [korma.core :as db]
            [clojure.tools.logging :as log]
            [clj-uuid :as uuid])
  (:use [korma.db]
        [repository.member]))

(declare subscriptions subscribees
         member-subscriptions member-subscribees)

(defn get-subscriptions-model
  [connection]
  (db/defentity subscriptions
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended))
  subscriptions)

(defn get-subscribees-model
  [connection]
  (db/defentity subscribees
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended))
  subscribees)

(defn get-member-subscriptions-model
  [connection]
  (db/defentity member-subscriptions
                (db/pk :id)
                (db/table :member_subscription)
                (db/database connection)
                (db/entity-fields :member_id :subscription_id)
                (db/has-one members {:fk :usr_id})
                (db/has-one subscriptions {:fk :usr_id}))
  member-subscriptions)

(defn get-member-subscribees-model
  [connection]
  (db/defentity member-subscribees
                (db/pk :id)
                (db/table :member_subscribee)
                (db/database connection)
                (db/entity-fields :member_id :subscribee_id)
                (db/has-one members {:fk :usr_id})
                (db/has-one subscribees {:fk :usr_id}))
  member-subscribees)

(defn select-member-subscriptions
  [model]
  (-> (db/select* model)
      (db/fields :id
                 :member_id
                 :subscription_id)))

(defn find-member-subscriptions-by
  "Find multiple member subscriptions"
  ; @see https://clojure.org/guides/destructuring#_where_to_destructure
  [{:keys [member-id subscriptions-ids]} model]
  (let [ids (if subscriptions-ids subscriptions-ids '(0))]
    (-> (select-member-subscriptions model)
        (db/where (and (= :member_id member-id)
                       (in :subscription_id ids)))
        (db/select))))

(defn select-member-subscribees
  [model]
  (-> (db/select* model)
      (db/fields :id
                 :member_id
                 :subscribee_id)))

(defn find-member-subscribees-by
  "Find multiple member subscribees"
  [{:keys [member-id subscribees-ids]} model]
  (let [ids (if subscribees-ids subscribees-ids '(0))]
    (-> (select-member-subscribees model)
        (db/where (and (= :member_id member-id)
                       (in :subscribee_id ids)))
        (db/select))))

(defn find-members-which-subscriptions-have-been-collected
  []
  (let [query (str "
                SELECT DISTINCT member_id AS `member-id`,
                usr_twitter_username AS `screen-name`,
                usr_twitter_id AS `member-twitter-id`
                FROM member_subscription ms
                INNER JOIN weaving_user m
                ON usr_id = member_id
                ORDER BY usr_twitter_username
              ")
        results (db/exec-raw [query] :results)]
    results))

(defn find-subscriptions-of-member-having-screen-name
  [screen-name]
  (let [query (str "
                SELECT DISTINCT subscription_id AS `member-id`,
                usr_twitter_username AS `screen-name`,
                usr_twitter_id AS `member-twitter-id`
                FROM member_subscription ms
                INNER JOIN weaving_user m
                ON usr_id = subscription_id
                WHERE usr_twitter_username = ?
                ORDER BY usr_twitter_username
              ")
        results (db/exec-raw [query [screen-name]] :results)]
    results))

(defn new-member-subscriptions
  [member-subscriptions model]
  (db/insert model (db/values member-subscriptions)))

(defn new-member-subscribees
  [member-subscribees model]
  (db/insert model (db/values member-subscribees)))

(defn create-member-subscription-values
  [member-id]
  (fn [subscription-id]
    {:id              (uuid/to-string (uuid/v1))
     :member_id       member-id
     :subscription_id subscription-id}))

(defn create-member-subscribee-values
  [member-id]
  (fn [subscribee-id]
    {:id            (uuid/to-string (uuid/v1))
     :member_id     member-id
     :subscribee_id subscribee-id}))

(defn ensure-subscriptions-exist-for-member-having-id
  [{member-id                          :member-id
    model                              :model
    matching-subscriptions-members-ids :matching-members-ids}]
  (let [existing-member-subscriptions (find-member-subscriptions-by {:member-id         member-id
                                                                     :subscriptions-ids matching-subscriptions-members-ids}
                                                                    model)
        existing-member-subscriptions-ids (map #(:subscription_id %) existing-member-subscriptions)
        member-subscriptions (clojure.set/difference (set matching-subscriptions-members-ids) (set existing-member-subscriptions-ids))
        missing-member-subscriptions (map (create-member-subscription-values member-id) member-subscriptions)]

    (when (pos? (count missing-member-subscriptions))
      (log/info (str "About to ensure " (count missing-member-subscriptions)
                     " subscriptions for member having id #" member-id " are recorded."))
      (new-member-subscriptions missing-member-subscriptions model)
      (log/info (str (count missing-member-subscriptions)
                     " subscriptions have been recorded successfully")))))

(defn ensure-subscribees-exist-for-member-having-id
  [{member-id                        :member-id
    model                            :model
    matching-subscribees-members-ids :matching-members-ids}]
  (let [existing-member-subscribees (find-member-subscribees-by {:member-id       member-id
                                                                 :subscribees-ids matching-subscribees-members-ids}
                                                                model)
        existing-member-subscribees-ids (map #(:subscribee_id %) existing-member-subscribees)
        member-subscribees (clojure.set/difference (set matching-subscribees-members-ids) (set existing-member-subscribees-ids))
        missing-member-subscribees (map (create-member-subscribee-values member-id) member-subscribees)]

    (when (pos? (count missing-member-subscribees))
      (log/info (str "About to ensure " (count missing-member-subscribees)
                     " subscribees for member having id #" member-id " are recorded."))
      (new-member-subscribees missing-member-subscribees model)
      (log/info (str (count missing-member-subscribees)
                     " subscribees have been recorded successfully")))))

