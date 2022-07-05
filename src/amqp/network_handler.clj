(ns amqp.network-handler
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [php_clj.core :refer [php->clj clj->php]])
  (:use [repository.entity-manager]
        [repository.member]
        [repository.member-subscription]
        [twitter.api-client]
        [twitter.member]))

(defn ensure-relationship-exists-for-member-having-id
  [{missing-members-ids :missing-members-ids
    member-id :member-id
    member-type :member-type}
   ensure-relationship-exists
   model member-model token-model token-type-model]
  (let [props (ensure-members-exist
                missing-members-ids
                token-model
                token-type-model
                member-model
                new-member-props-from-json)
        existing-members (find-members-from-props props member-model)
        existing-members-ids (set (map #(:twitter-id %) existing-members))
        ; Member Twitter ids singleton belonging to the set of existing members Twitter ids are filtered out
        first-seen-props (filter #(not (clojure.set/subset? #{(:twitter-id %)} existing-members-ids)) props)
        new-members (bulk-insert-new-members first-seen-props member-model)
        _ (doall (map (get-new-member-logger member-type) new-members))
        new-members-ids (pmap #(:id %) new-members)
        relationships (ensure-relationship-exists
                        {:member-id member-id
                         :model model
                         :matching-members-ids new-members-ids})]
    relationships))

(defn process-subscriptions
  [member-id screen-name member-subscription-model token-model token-type-model member-model on-reached-api-limit]
  (let [subscriptions-ids (get-subscriptions-of-member screen-name token-model token-type-model on-reached-api-limit)
        matching-subscriptions-members (find-members-having-ids subscriptions-ids member-model)
        matching-subscriptions-members-ids (map-get-in :id matching-subscriptions-members)
        missing-subscriptions-members-ids (deduce-ids-of-missing-members matching-subscriptions-members subscriptions-ids)]

    (if (pos? (count missing-subscriptions-members-ids))
      (ensure-relationship-exists-for-member-having-id
        {:member-id member-id
         :member-type "subscription"
         :missing-members-ids missing-subscriptions-members-ids}
        ensure-subscriptions-exist-for-member-having-id
        member-subscription-model
        member-model
        token-model
        token-type-model)
      (log/info (str "No member missing from subscriptions of member \"" screen-name "\"")))

    (ensure-subscriptions-exist-for-member-having-id {:member-id member-id
                                                      :model member-subscription-model
                                                      :matching-members-ids matching-subscriptions-members-ids})))

(defn process-subscribees
  [member-id screen-name member-subscribee-model token-model token-type-model member-model]
  (let [subscribees-ids (get-subscribees-of-member screen-name token-model token-type-model)
        matching-subscribees-members (find-members-having-ids subscribees-ids member-model)
        matching-subscribees-members-ids (map-get-in :id matching-subscribees-members)
        missing-subscribees-members-ids (deduce-ids-of-missing-members matching-subscribees-members subscribees-ids)]

    (if (pos? (count missing-subscribees-members-ids))
      (ensure-relationship-exists-for-member-having-id
        {:member-id member-id
         :member-type "subscribee"
         :missing-members-ids missing-subscribees-members-ids}
        ensure-subscribees-exist-for-member-having-id
        member-subscribee-model
        member-model
        token-model
        token-type-model)
      (log/info (str "No member missing from subscribees of member \"" screen-name "\"")))

    (ensure-subscribees-exist-for-member-having-id {:member-id member-id
                                                    :model member-subscribee-model
                                                    :matching-subscribees-members-ids matching-subscribees-members-ids})))

(defn process-network
  [payload entity-manager]
  (let [{members :members
         token :token
         token-type :token-type
         member-subscriptions :member-subscriptions
         member-subscribees :member-subscribees} entity-manager
        screen-name (first (json/read-str (php->clj (String. payload  "UTF-8"))))
        {member-id :id
         twitter-id :twitter-id} (get-id-of-member-having-username screen-name members token token-type)
        subscribees-processor (fn [] (process-subscribees member-id screen-name member-subscribees token token-type members))]
    (try
      (process-subscriptions member-id screen-name member-subscriptions token token-type members subscribees-processor)
      (subscribees-processor)
      (catch Exception e
        (when (= (.getMessage e) error-unauthorized-friends-ids-access)
          (guard-against-exceptional-member {:id member-id
                                             :screen_name screen-name
                                             :twitter-id twitter-id
                                             :is-not-found 0
                                             :is-protected 0
                                             :is-suspended 1
                                             :total-subscribees 0
                                             :total-subscriptions 0} members))))))
