(ns command.generate-timely-statuses
  (:require [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [environ.core :refer [env]])
  (:use [repository.entity-manager]
        [repository.aggregate]
        [repository.timely-status]
        [twitter.date]
        [twitter.status]))

(def ^:dynamic *generate-timely-statuses-enabled-loggin* false)

(defn assoc-time-range
  [status]
  (let [time-range (get-time-range (:publication-date-time status))]
    (assoc status :time-range time-range)))

(defn generate-timely-statuses-from-statuses-props
  [{aggregate-name                :aggregate-name
    ids                           :ids
    {timely-status-model :timely-status
     status-model        :status} :models} week year]
  (let [statuses (find-timely-statuses-props-for-aggregate ids)
        find #(find-by-statuses-ids % timely-status-model status-model)
        filtered-statuses (filter-out-known-statuses find statuses)
        statuses-props (pmap assoc-time-range filtered-statuses)
        new-timely-statuses (bulk-insert statuses-props aggregate-name timely-status-model status-model)]
    (when *generate-timely-statuses-enabled-loggin*
      (doall (pmap #(log/info (str "A timely status has been added for member \""
                                   (:member-name %) "\"")) new-timely-statuses)))
    (log/info (str (count new-timely-statuses) " new timely statuses have been added for \""
                   aggregate-name
                   "\" from week #" week
                   " and year " year))
    new-timely-statuses))

(defn generate-timely-statuses
  ([week year]
   (let [press-aggregate-name (:press (edn/read-string (:aggregate env)))]
     (generate-timely-statuses {:week           week
                                :year           year
                                :aggregate-name press-aggregate-name})))
  ([{year           :year
     week           :week
     aggregate-id   :aggregate-id
     aggregate-name :aggregate-name}]
   (let [generation-params {:models (get-entity-manager (:database env))}
         {total-timely-status :total-timely-statuses
          statuses-ids        :statuses-ids} (get-timely-statuses-for-aggregate {:aggregate-id     aggregate-id
                                                                                 :aggregate-name   aggregate-name
                                                                                 :publication-week week
                                                                                 :publication-year year})
         _ (when *generate-timely-statuses-enabled-loggin*
             (log/info (str total-timely-status " potential timely statuses have be counted.")))
         timely-statuses-params (assoc generation-params :ids statuses-ids)
         timely-statuses-params (assoc timely-statuses-params :aggregate-name aggregate-name)
         statuses (generate-timely-statuses-from-statuses-props timely-statuses-params week year)]
     statuses)))

(defn generate-timely-statuses-for-aggregate
  [{year           :year
    aggregate-id   :aggregate-id
    aggregate-name :aggregate-name} & [in-parallel]]
  (if in-parallel
    (let [weeks (take 52 (iterate inc 0))]
      (doall
        (pmap
          #(generate-timely-statuses {:aggregate-name aggregate-name
                                      :aggregate-id   aggregate-id
                                      :week           %
                                      :year           year})
          weeks)))
    (loop [week 0]
      (when (< week 52)
        (generate-timely-statuses {:aggregate-name aggregate-name
                                   :aggregate-id   aggregate-id
                                   :week           week
                                   :year           year})
        (recur (inc week))))))

(defn generate-timely-statuses-for-member
  [member year]
  (let [_ (get-entity-manager (:database env))
        aggregates (get-member-aggregates-by-screen-name member)
        statuses (doall
                   (pmap
                     #(generate-timely-statuses-for-aggregate
                        {:year           year
                         :aggregate-id   (:aggregate-id %)
                         :aggregate-name (:aggregate-name %)} :in-parallel)
                     aggregates))]
    statuses))