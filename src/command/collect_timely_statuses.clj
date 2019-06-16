(ns command.collect-timely-statuses
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
    [profiling.execution-time]
    [twitter.date]
    [twitter.status]))

(def ^:dynamic *generate-timely-statuses-enabled-logging* false)

(defn assoc-time-range
  [status]
  (let [time-range (get-time-range (:publication-date-time status))]
    (assoc status :time-range time-range)))

(defn generate-timely-statuses-from-statuses-props
  [{aggregate-name :aggregate-name
    ids            :ids
    models         :models} week year]
  (let [statuses (find-timely-statuses-props-for-aggregate ids)
        find #(find-timely-statuses-by-constraints % aggregate-name models)
        filtered-statuses (filter-out-known-statuses find statuses)
        statuses-props (pmap assoc-time-range filtered-statuses)
        _ (log/info (str "About to generate timely statuses from " year " for \"" aggregate-name "\""))
        new-timely-statuses (bulk-insert statuses-props models)
        total-timely-statuses (count new-timely-statuses)]
    (when *generate-timely-statuses-enabled-logging*
      (doall (pmap #(log/info (str "A timely status has been added for member \""
                                   (:member-name %) "\" (" (:id %) ")")) new-timely-statuses)))
    (when (pos? total-timely-statuses)
      (log/info (str total-timely-statuses " new timely statuses have been added for \""
                     aggregate-name
                     (if week
                       (str "\" from week #" week " and year " year)
                       (str "\" from year " year)))))
    new-timely-statuses))

(defn generate-timely-statuses-from-subscriptions-of-member
  [{aggregate-id :aggregate-id
    models       :models}]
  (let [timely-statuses (find-missing-timely-statuses-from-aggregate aggregate-id)
        statuses-props (pmap assoc-time-range timely-statuses)
        new-timely-statuses (bulk-insert statuses-props models)
        total-timely-statuses (count new-timely-statuses)]
    (when *generate-timely-statuses-enabled-logging*
      (doall (pmap #(log/info (str "A timely status has been added for member \""
                                   (:member-name %) "\" (" (:id %) ")")) new-timely-statuses)))
    (cond
      (> total-timely-statuses 1)
      (log/info (str total-timely-statuses " new timely statuses have been added for aggregate #" aggregate-id))
      (= 1 total-timely-statuses)
      (log/info (str total-timely-statuses " new timely status has been added for aggregate #" aggregate-id))
      :else
      (log/info (str "No new timely status has been added for aggregate #" aggregate-id)))
    new-timely-statuses))

(defn collect-timely-statuses
  ([week year]
   (let [press-aggregate-name (:press (edn/read-string (:aggregate env)))]
     (collect-timely-statuses {:week           week
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
             (log/info (str total-timely-status " potential timely statuses have be counted for year " year ".")))
         timely-statuses-params (assoc generation-params :ids statuses-ids)
         timely-statuses-params (assoc timely-statuses-params :aggregate-name aggregate-name)
         statuses (generate-timely-statuses-from-statuses-props timely-statuses-params week year)]
     statuses)))

(declare build-relationships)

(defn handle-list
  [list-spec]
  (try
    (profile #(-> list-spec process-lists))
    (profile #(-> list-spec build-relationships))
    (catch Exception e
      (log/error (str "An error occurred with message " (.getMessage e))))))

(defn consolidate-timely-statuses-from-subscriptions-for-member
  [member]
  (let [entity-manager (get-entity-manager (:database env))
        aggregates (get-member-aggregates-by-screen-name member)
        statuses (doall
                   (pmap
                     #(generate-timely-statuses-from-subscriptions-of-member
                        {:aggregate-id (:aggregate-id %)
                         :models       entity-manager})
                     aggregates))]
    statuses))

(defn parse-status-publication-date
  [aggregate]
  (let [prop :last-status-publication-date
        publication-date (c/from-long (get aggregate prop))
        aggregate (assoc (dissoc aggregate prop) prop publication-date)]
    aggregate))

(defn sort-by-status-publication-date
  [aggregates]
  (let [aggregates-having-parsed-publication-dates (pmap
                                                     parse-status-publication-date
                                                     aggregates)]
    (sort-by :last-status-publication-date aggregates-having-parsed-publication-dates)))

(defn collect-timely-statuses-for-member-subscriptions
  [member]
  (let [entity-manager (get-entity-manager (:database env))
        aggregates (get-member-aggregates-by-screen-name member)
        sorted-aggregates (sort-by-status-publication-date aggregates)
        statuses (doall
                   (pmap
                     #(handle-list
                        {:screen-name                   (:member-name %)
                         :aggregate-id                  (:aggregate-id %)
                         :entity-manager                entity-manager
                         :unavailable-aggregate-message error-unavailable-aggregate})
                     sorted-aggregates))]
    statuses))

(defn collect-timely-statuses-for-aggregates-matching-pattern
  [param entity-manager & [aggregate-getter]]
  (let [getter (if
                 (some? aggregate-getter)
                 aggregate-getter
                 get-aggregates-having-name-prefix)
        aggregates (getter param)
        aggregates-grouped-by-screen-name (group-by #(:member-name %) aggregates)
        sortable-aggregates (pmap #(first (last %)) aggregates-grouped-by-screen-name)
        sorted-aggregates (sort-by-status-publication-date sortable-aggregates)
        statuses (doall
                   (pmap
                     #(handle-list
                        {:screen-name                   (:member-name %)
                         :aggregate-id                  (:aggregate-id %)
                         :entity-manager                entity-manager
                         :unavailable-aggregate-message error-unavailable-aggregate})
                     sorted-aggregates))]
    statuses))

(defn collect-timely-statuses-from-aggregates
  []
  (let [alphabet (map char (range 97 123))
        entity-manager (get-entity-manager (:database env))
        _ (doall
            (map
              #(collect-timely-statuses-for-aggregates-matching-pattern % entity-manager)
              alphabet))]))

(defn collect-timely-statuses-from-aggregate
  [aggregate-name]
  (let [entity-manager (get-entity-manager (:database env))
        _ (collect-timely-statuses-for-aggregates-matching-pattern
            aggregate-name
            entity-manager
            get-aggregates-sharing-name)]))

(defn collect-timely-statuses-for-member
  [member]
  (let [entity-manager (get-entity-manager (:database env))
        aggregate (get-member-aggregate member)
        statuses (handle-list
                   {:screen-name                   member
                    :aggregate-id                  (:aggregate-id aggregate)
                    :entity-manager                entity-manager
                    :unavailable-aggregate-message error-unavailable-aggregate})]
    statuses))

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
           aggregate-model        :aggregate :as models} entity-manager
          aggregate (get-aggregate-by-id aggregate-id aggregate-model unavailable-aggregate-message)
          aggregate-name (:name aggregate)
          member (first (find-member-by-screen-name screen-name member-model))
          found-statuses (find-statuses-for-aggregate-authored-by [(:screen-name member)] aggregate-id)
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
          new-timely-statuses (when (> total-new-statuses 0)
                                (bulk-insert-timely-statuses-from-aggregate aggregate-id))
          log-message (if (< 0 total-new-statuses)
                        (str
                          "There are " (count new-timely-statuses)
                          " new timely statuses for \"" aggregate-name "\" (" screen-name ") aggregate")
                        (str
                          "No timely status is to be generated for aggregate \""
                          aggregate-name "\""))]
      (log/info log-message))))
