(ns command.recommend-subscriptions
  (:require [environ.core :refer [env]])
  (:use [recommendation.distance]
        [repository.entity-manager]))

(defn recommend-subscriptions-from-member-subscription-history
  "Port of command written in PHP to recommend subscriptions from the existing subscribing history of a member"
  ; @see https://github.com/thierrymarianne/daily-press-review/pull/91
  [screen-name]
  (let [_ (get-entity-manager (:database env))]
    (let [distinct-subscriptions-ids (find-distinct-ids-of-subscriptions)
          {identity-vector :member-vector
           screen-name :screen-name
           total-subscriptions :total-subscriptions} (reduce-member-vector screen-name distinct-subscriptions-ids)
          other-members-subscriptions (find-members-closest-to-member-having-screen-name total-subscriptions)
          other-members (map
                          (reduce-member-vector-against-overall-subscriptions
                            distinct-subscriptions-ids)
                          other-members-subscriptions)
          distances-to-other-members-vectors (get-distance-from-others identity-vector other-members)
          sorted-distances (sort-by #(:distance %) distances-to-other-members-vectors)]
      (doall (map (get-distance-logger screen-name) sorted-distances)))))
