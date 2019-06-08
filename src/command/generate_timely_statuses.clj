(ns command.generate-timely-statuses
  (:require
    [clj-time.core :as t]
    [clj-time.coerce :as c]
    [clojure.tools.logging :as log]
    [clojure.edn :as edn]
    [environ.core :refer [env]])
  (:use
    [amqp.handling-errors]
    [amqp.status-handler]
    [repository.entity-manager]
    [repository.aggregate]
    [repository.status]
    [repository.timely-status]
    [twitter.date]
    [twitter.status]))

(def ^:dynamic *generate-timely-statuses-enabled-logging* false)

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
    (when *generate-timely-statuses-enabled-logging*
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
         _ (when *generate-timely-statuses-enabled-logging*
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

(declare build-relationships)

(defn handle-list
  [{screen-name                   :screen-name
    aggregate-id                  :aggregate-id
    entity-manager                :entity-manager
    unavailable-aggregate-message :unavailable-aggregate-message}]
  (process-lists
    {:screen-name                   screen-name
     :aggregate-id                  aggregate-id
     :entity-manager                entity-manager
     :unavailable-aggregate-message unavailable-aggregate-message})
  (build-relationships
    {:screen-name                   screen-name
     :aggregate-id                  aggregate-id
     :entity-manager                entity-manager
     :unavailable-aggregate-message unavailable-aggregate-message}))

(defn generate-timely-statuses-for-member-subscriptions
  [member]
  (let [entity-manager (get-entity-manager (:database env))
        aggregates (get-member-aggregates-by-screen-name member)
        statuses (doall
                   (pmap
                     #(handle-list
                        {:screen-name                   member
                         :aggregate-id                  (:aggregate-id %)
                         :entity-manager                entity-manager
                         :unavailable-aggregate-message error-unavailable-aggregate})
                     aggregates))]
    statuses))

(defn generate-timely-statuses-for-member
  [member]
  (let [entity-manager (get-entity-manager (:database env))
        aggregate (get-member-aggregate member)
        statuses (handle-list
                   {:screen-name                   member
                    :aggregate-id                  (:aggregate-id aggregate)
                    :entity-manager                entity-manager
                    :unavailable-aggregate-message error-unavailable-aggregate})]
    statuses))

(defn generate-timely-statuses-for-year
  [{aggregate-id   :aggregate-id
    aggregate-name :aggregate-name
    year           :year}]
  (log/info (str "About to generate timely statuses from " year " for \"" aggregate-name "\""))
  (generate-timely-statuses-for-aggregate
    {:aggregate-name aggregate-name
     :aggregate-id   aggregate-id
     :year           year}
    :in-parallel))

(defn build-relationships
  "Build relationships missing between statuses and aggregates"
  [{screen-name                   :screen-name
    aggregate-id                  :aggregate-id
    entity-manager                :entity-manager
    unavailable-aggregate-message :unavailable-aggregate-message}]
  ; do not process aggregate with id #1 (taken care of by another command)
  (when (not= 1 aggregate-id)
    (let [{member-model           :members
           status-model           :status
           status-aggregate-model :status-aggregate
           aggregate-model        :aggregate} entity-manager
          aggregate (get-aggregate-by-id aggregate-id aggregate-model unavailable-aggregate-message)
          aggregate-name (:name aggregate)
          member (first (find-member-by-screen-name screen-name member-model))
          found-statuses (find-statuses-for-aggregate-authored-by [(:screen-name member)] aggregate-name)
          {total-new-relationships :total-new-relationships} (new-relationships
                                                               aggregate
                                                               found-statuses
                                                               status-aggregate-model
                                                               status-model)
          total-new-statuses (count found-statuses)
          _ (when (pos? total-new-relationships)
              (log-new-relationships-between-aggregate-and-statuses
                total-new-relationships
                total-new-statuses
                aggregate-name))
          statuses-sorted-by-date (sort-by :created-at found-statuses)
          first-status (first statuses-sorted-by-date)
          since (t/year (c/from-long (:created-at first-status)))
          last-status (last statuses-sorted-by-date)
          until (t/year (c/from-long (:created-at last-status)))]
      (let [years (take (inc (- until since)) (iterate inc since))]
        (doall
          (pmap
            #(generate-timely-statuses-for-year
               {:year           %
                :aggregate-name aggregate-name
                :aggregate-id   aggregate-id})
            years))))))
