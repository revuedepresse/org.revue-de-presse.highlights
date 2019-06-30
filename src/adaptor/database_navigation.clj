(ns adaptor.database-navigation
  (:require [environ.core :refer [env]])
  (:use [formatting.formatter]
        [repository.aggregate]
        [repository.keyword]
        [repository.status]
        [repository.status-aggregate]
        [repository.entity-manager]))

(defn adapt-results
  [{props :props finder :finder formatter :formatter}]
  (let [results (let [get-models #(get-entity-manager (:database env))]
                  (finder get-models))
        props (if (some? props)
                props
                (keys (first results)))]
    {:provides  props
     :result    (map
                  #(select-keys % props)
                  results)
     :formatter formatter}))

(defn list-aggregates
  [& [finder]]
  (let [finder (fn [get-models]
                 (get-models)
                 (if (some? finder)
                   (finder)
                   (find-all-aggregates)))]
    (adapt-results {:finder    finder
                    :formatter (get-indexed-prop-formatter :aggregate-name :aggregate-id)})))

(defn list-keyword-aggregates
  []
  (list-aggregates #(find-keyword-aggregates)))

(defn list-aggregates-containing-member
  [screen-name]
  (list-aggregates #(find-aggregates-enlisting-member screen-name)))

(defn list-keywords-by-aggregate
  [aggregate-name & [finder]]
  (let [finder (fn [get-models]
                 (let [_ (get-models)
                       finder (if (some? finder)
                                finder
                                find-keywords-by-aggregate-name)]
                   (reverse (finder aggregate-name))))]
    (adapt-results {:props     [:occurrences :keyword :aggregate-name :aggregate-id]
                    :finder    finder
                    :formatter (get-indexed-prop-formatter :aggregate-name :aggregate-id)})))

(defn list-mentions-by-aggregate
  [aggregate-name]
  (list-keywords-by-aggregate aggregate-name find-mentions-by-aggregate-name))

(defn list-member-statuses
  [screen-name]
  (adapt-results {:props     [:status-id :status-twitter-id :text :created-at :screen-name]
                  :finder    (fn [get-models]
                               (let [{status-model :status} (get-models)]
                                 (reverse (find-statuses-by-screen-name screen-name status-model))))
                  :formatter (get-status-formatter :screen-name :text :created-at)}))

(defn list-aggregate-statuses
  [aggregate-name]
  (adapt-results {:props     [:status-id :status-twitter-id :text :created-at :screen-name :aggregate-name]
                  :finder    (fn [get-models]
                               (reverse (find-statuses-by-aggregate-name
                                          aggregate-name
                                          (get-models))))
                  :formatter (get-status-formatter :screen-name :text :created-at)}))

(defn list-statuses-containing-keyword
  [keyword]
  (adapt-results {:props     [:status-id :status-twitter-id :text :created-at :screen-name :aggregate-name]
                  :finder    (fn [get-models]
                               (reverse (find-statuses-containing-keyword
                                          keyword
                                          (get-models))))
                  :formatter (get-status-formatter :screen-name :text :created-at)}))

(defn list-members-in-aggregate
  [aggregate-name]
  (adapt-results {:props     [:aggregate-name :screen-name :member-id :member-twitter-id]
                  :finder    (fn [get-models]
                               (let [_ get-models]
                                 (find-members-by-aggregate aggregate-name)))
                  :formatter (get-indexed-prop-formatter :screen-name :member-twitter-id)}))

(defn get-member-description
  [screen-name]
  (adapt-results {:props     [:description :screen-name :member-id]
                  :finder    (fn [get-models]
                               (let [{member-model :members} (get-models)]
                                 (find-member-by-screen-name screen-name member-model)))
                  :formatter (get-indexed-prop-formatter :screen-name :member-twitter-id)}))