(ns command.collect-timely-statuses
  (:require
    [clj-time.coerce :as c]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [utils.error-handler :as error-handler])
  (:use
    [amqp.handling-errors]
    [amqp.status-handler]
    [repository.entity-manager]
    [repository.publishers-list]
    [repository.member]
    [repository.status]
    [repository.timely-status]
    [profiling.execution-time]
    [twitter.date]
    [twitter.status]
    [utils.string]))

(declare build-relationships)

(defn handle-list
  [list-spec]
  (try
    (profile #(-> list-spec process-lists))
    (profile #(-> list-spec build-relationships))
    (catch Exception e
      (cond
        (= (.getMessage e) error-unavailable-aggregate) (log/info (str "Could not find aggregate #" (:aggregate-id list-spec)))
        :else (error-handler/log-error
                e
                "An error occurred with message ")))))

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
  [screen-name]
  (let [entity-manager (get-entity-manager (:database env))
        aggregates (get-member-aggregates-by-screen-name screen-name)
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
  [letter & [reverse-order]]
  (let [alphabetic-characters (map :letter (get-alphabet))
        alphabet (if (some? letter)
                   (apply list [letter])
                   (if (some? reverse-order)
                     (reverse alphabetic-characters)
                     alphabetic-characters))
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
  [screen-name]
  (let [entity-manager (get-entity-manager (:database env))
        aggregate (get-member-aggregate screen-name)
        statuses (handle-list
                   {:screen-name                   screen-name
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
           aggregate-model        :aggregate} entity-manager
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
                          "There are " new-timely-statuses
                          " new timely statuses for \"" aggregate-name "\" (" screen-name ") aggregate")
                        (str
                          "No timely status is to be generated for aggregate \""
                          aggregate-name "\""))]
      (log/info log-message))))
