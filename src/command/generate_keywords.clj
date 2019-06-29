(ns command.generate-keywords
  (:require [environ.core :refer [env]]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn])
  (:use [repository.entity-manager]
        [repository.aggregate]
        [repository.highlight]
        [repository.timely-status]
        [repository.keyword]
        [utils.string]))

(defn assoc-keyword
  [keyword-props]
  (fn [keyword]
    (let [props (dissoc keyword-props :status-api-document)
          associated-keywords (assoc props :keyword keyword)]
      associated-keywords)))

(defn from-highlight
  []
  (fn [highlight]
    (try
      (let [all-keywords (select-keys
                           highlight
                           [:aggregate-id
                            :aggregate-name
                            :status-id
                            :publication-date-time
                            :member-id
                            :status-id
                            :status-api-document])
            decoded-document (json/read-str (:status-api-document all-keywords))
            status (if (contains? decoded-document "text")
                     (get decoded-document "text")
                     (get decoded-document "full_text"))
            keywords (explode #"\s+" status)
            associated-keywords (map (assoc-keyword all-keywords) keywords)]
        associated-keywords)
      (catch Exception e (log/error (.getMessage e))))))

(defn assoc-occurrences
  [[_ occurrences]]
  (let [keyword-props (assoc
                        (first occurrences)
                        :occurrences
                        (count occurrences))]
    keyword-props))

(defn new-keywords-from-props
  [props models & [log-message find-keywords]]
  (let [{status-model  :status
         member-model  :members
         keyword-model :hashtag} models
        log-message-ending (if (some? log-message)
                             log-message
                             "")
        statuses-ids (pmap :status-id props)
        matching-keywords (find-keywords-for-statuses-ids
                            statuses-ids
                            keyword-model
                            member-model
                            status-model)
        matching-statuses-ids (pmap #(:status-id %) matching-keywords)
        remaining-props (remove
                          #(clojure.set/subset?
                             #{(:status-id %)}
                             (set matching-statuses-ids))
                          props)
        keyword-collection (flatten (pmap (from-highlight) remaining-props))
        grouped-keywords (group-by #(str (:status-id %) "-" (:keyword %)) keyword-collection)
        counted-keywords (pmap assoc-occurrences grouped-keywords)
        _ (bulk-insert-new-keywords
            counted-keywords
            models
            find-keywords)]
    (log/info (str (count counted-keywords) " keywords have been generated " log-message-ending))
    counted-keywords))

(defn generate-keywords-for-all-aggregates
  ([date]
   (generate-keywords-for-all-aggregates date {}))
  ([date & [{week :week year :year}]]
   (let [press-aggregate-name (:press (edn/read-string (:aggregate env)))
         models (get-entity-manager (:database env))
         highlights (find-highlighted-statuses-for-aggregate-published-at {:date           date
                                                                           :week           week
                                                                           :year           year
                                                                           :aggregate-name press-aggregate-name
                                                                           :not-in         true
                                                                           :models         models})
         new-keywords (new-keywords-from-props
                        highlights
                        models
                        (str "for \"" press-aggregate-name "\"")
                        :find-keywords)
         props (get-keywords-props)]
     {:provides  props
      :result    new-keywords
      :formatter #(str
                    (:keyword %)
                    " for aggregate " (:aggregate-name %) " #" (:aggregate-id %)
                    " and member #" (:member-id %)
                    "(" (:occurrences %) ")"
                    )})))

(defn generate-keywords-for-aggregate
  [param & [models timely-status-finder log-message-ending]]
  (let [models (if (some? models)
                 models
                 (get-entity-manager (:database env)))
        timely-status-finder (if (some? timely-status-finder)
                               timely-status-finder
                               find-timely-statuses-by-aggregate-name)
        timely-statuses (timely-status-finder param models)
        new-keywords (new-keywords-from-props
                       timely-statuses
                       models
                       log-message-ending)]
    new-keywords))

(defn generate-keywords-from-aggregate
  [aggregate timely-status-finder models]
  (let [aggregate-id (:aggregate-id aggregate)
        aggregate-name (:aggregate-name aggregate)
        screen-name (:screen-name aggregate)
        log-message-ending (str "for \"" aggregate-name "\" (" screen-name ") aggregate")]
    (println (str "About to generate keywords " log-message-ending))
    (generate-keywords-for-aggregate aggregate-id models timely-status-finder log-message-ending)))

(defn generate-keywords-for-aggregates-sharing-name
  [aggregate-name]
  (let [models (get-entity-manager (:database env))
        aggregates (get-aggregates-sharing-name aggregate-name)
        _ (doall (pmap
                   #(generate-keywords-from-aggregate % find-timely-statuses-by-aggregate-id models)
                   aggregates))]))