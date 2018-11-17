(ns recommendation.distance
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]))

;    /**
;     * @param $initialVector
;     * @return int|mixed
;     */
;    private function reduceMemberVector($initialVector)
;    {
;        $positions = array_values(explode(',', $initialVector));
;        $positions = array_map('intval', $positions);
;        $flippedPositions = array_flip($positions);
;
;        return array_map(function ($subscription, $index) use ($flippedPositions) {
;            if (!$subscription) {
;                return null;
;            }
;
;            if (array_key_exists($index, $flippedPositions)) {
;                // Normalization
;                return $subscription / $subscription;
;            }
;
;            return null;
;        }, $this->allSubscriptions, array_keys($this->allSubscriptions));
;    }

(defn explode
  [sep subject]
  (apply vector (clojure.string/split subject sep)))

(defn normalize-contribution
  [contributions]
  (fn
    [subscription subscription-index]
    (let [is-subset-of (set/subset? #{subscription} contributions)]
      (if is-subset-of
        (log/info "Subscribing contribution #" subscription " is a subset of the member subscriptions.")
        (log/info "Subscribing contribution #" subscription " do not belong to the member subscriptions."))
      (when is-subset-of 1))))

(defn reduce-member-vector
  [member-subscriptions, all-subscriptions]
  (let [split-member-subscriptions-ids (explode #"," member-subscriptions)
        member-subscriptions-ids (map #(Long/parseLong %) split-member-subscriptions-ids)
        all-subscriptions-indices (take (count all-subscriptions) (iterate inc 0))]
    (doall (map (normalize-contribution (set member-subscriptions-ids)) all-subscriptions all-subscriptions-indices))))

