(ns command.generate-timely-statuses
  (:require [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [environ.core :refer [env]])
  (:use [repository.entity-manager]
        [repository.timely-status]
        [twitter.date]
        [twitter.status]))

(defn assoc-time-range
  [status]
  (let [time-range (get-time-range (:publication-date-time status))]
    (assoc status :time-range time-range)))

(defn generate-timely-statuses-from-statuses-props
  [{ids :ids
    {timely-status-model :timely-status
    status-model :status} :models}]
  (let [statuses (find-timely-statuses-props-for-aggregate ids)
      find #(find-by-statuses-ids % timely-status-model status-model)
      filtered-statuses (filter-out-known-statuses find statuses)
      statuses-props (map assoc-time-range filtered-statuses)
      new-timely-statuses (bulk-insert statuses-props timely-status-model status-model)]
    (doall (map #(log/info (str "A timely status has been added for member \""
                                (:member-name %) "\"")) new-timely-statuses))
    (log/info (str (count new-timely-statuses) " new timely statuses have been added"))
    new-timely-statuses))

(defn generate-timely-statuses
  [week year]
  (let [press-aggregate-name (:press (edn/read-string (:aggregate env)))
        generation-params {:models (get-entity-manager (:database env))}
        {total-timely-status :total-timely-statuses
         statuses-ids :statuses-ids} (get-timely-statuses-for-aggregate {:aggregate-name press-aggregate-name
                                                                         :publication-week week
                                                                         :publication-year year})
        _ (log/info (str total-timely-status " potential timely statuses have be counted."))
        statuses (generate-timely-statuses-from-statuses-props (assoc generation-params :ids statuses-ids))]
    statuses))