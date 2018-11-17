(ns recommendation.distance
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.math.numeric-tower :as math])
  (:use [repository.entity-manager]
        [utils.string]))

(def *enable-logging* false)


(defn normalize-contribution
  [contributions enabled-logging]
  (fn
    [subscription subscription-index]
    (let [is-subset-of (set/subset? #{subscription-index} contributions)]
      (when enabled-logging
        (if is-subset-of
          (log/info (str "Subscribing contribution #" subscription " (index "
                      subscription-index ") is a subset of the member subscriptions."))
          (log/info (str "Subscribing contribution #" subscription " (index "
                      subscription-index") do not belong to the member subscriptions."))))
      (when is-subset-of 1))))

(defn reduce-member-vector
  ([screen-name all-subscriptions]
    (let [{raw-subscriptions-ids :raw-subscriptions-ids} (find-member-subscriptions screen-name)]
      (reduce-member-vector raw-subscriptions-ids screen-name all-subscriptions)))
  ([raw-subscriptions-ids screen-name all-subscriptions]
    (let [split-member-subscriptions-ids (explode #"," raw-subscriptions-ids)
          member-subscriptions-ids (pmap #(Long/parseLong %) split-member-subscriptions-ids)
          all-subscriptions-indices (take (count all-subscriptions) (iterate inc 0))
          member-vector (map (normalize-contribution
                                (set member-subscriptions-ids)
                                *enable-logging*)
                             all-subscriptions all-subscriptions-indices)
          total-subscriptions (count (filter #(when % %) member-vector))]
      (when *enable-logging*
        (log/info (str "There are " (count member-vector) " subscriptions."))
        (log/info (str "Subscriptions of \"" screen-name "\" counts " total-subscriptions " contributions.")))
      {:total-subscriptions total-subscriptions
       :screen-name screen-name
       :member-vector member-vector})))

(defn reduce-member-vector-against-overall-subscriptions
  [subscriptions-ids]
  (fn [record]
    (reduce-member-vector (:subscription_ids record) (:identifier record) subscriptions-ids)))

(defn power-of-difference
  [x y]
  (let [[x1 y2] [(if x x 0) (if y y 0)]
        absolute-value-of-difference (- x1 y2)
        res (math/expt 2 absolute-value-of-difference)]
    res))

(defn get-distance-from
  [from-vector]
  (fn
  [vector-props]
  (let [{screen-name :screen-name
         to-vector :member-vector} vector-props
        power-of-differences (map power-of-difference from-vector to-vector)
        distance (math/sqrt (apply + power-of-differences))]
    (when *enable-logging*
      (log/info (str "Distance to reference vector for member \""
                     screen-name "\" is " distance)))
    {:distance distance
     :screen-name screen-name})))

(defn get-distance-printer
  [screen-name]
  (fn
  [distance]
  (println (str "The distance between \"" (:screen-name distance)
                "\" and \"" screen-name "\" is " (:distance distance)))))

(defn get-distance-from-others
  [reference-vector other-vectors]
  (map (get-distance-from reference-vector) other-vectors))