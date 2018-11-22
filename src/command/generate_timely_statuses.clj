(ns command.generate-timely-statuses
  (:require [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [environ.core :refer [env]])
  (:use [repository.entity-manager]
        [repository.timely-status]
        [twitter.date]
        [twitter.status]
        ))


(defn assoc-time-range
  [status]
  (let [publication-date-cols (get-publication-dates-of-statuses (:publication-date-time status))
        parsed-publication-date (:parsed-publication-date publication-date-cols)
        time-range (get-time-range parsed-publication-date)]
    (assoc status :time-range time-range)))

(defn generate-timely-statuses
  [week year]
  (let [{timely-status-model :timely-status
         status-model :status} (get-entity-manager (:database env))
        press-aggregate-name (:press (edn/read-string (:aggregate env)))
        statuses (find-raw-statuses-for-aggregate press-aggregate-name week year)
        find #(find-by-ids % timely-status-model status-model)
        filtered-statuses (filter-out-known-statuses find statuses)
        statuses-props (map assoc-time-range filtered-statuses)
        new-timely-statuses (bulk-insert statuses-props timely-status-model status-model)]
    (doall (map #(log/info (str "A timely status has been added for " (:status-id %))) new-timely-statuses))
    new-timely-statuses))