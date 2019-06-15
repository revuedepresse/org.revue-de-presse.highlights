(ns command.generate-keywords
  (:require [environ.core :refer [env]]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn])
  (:use [repository.entity-manager]
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
  [props models & [find-keywords]]
  (let [{status-model  :status
         member-model  :members
         keyword-model :hashtag} models
        statuses-ids (map :status-id props)
        matching-keywords (find-keywords-for-statuses-ids
                            statuses-ids
                            keyword-model
                            member-model
                            status-model)
        matching-statuses-ids (map #(:status-id %) matching-keywords)
        remaining-props (remove
                          #(clojure.set/subset?
                             #{(:status-id %)}
                             (set matching-statuses-ids))
                          props)
        keyword-collection (flatten (doall (map (from-highlight) remaining-props)))
        grouped-keywords (group-by #(str (:status-id %) "-" (:keyword %)) keyword-collection)
        counted-keywords (doall (map assoc-occurrences grouped-keywords))
        new-keywords (bulk-insert-new-keywords
                       counted-keywords
                       models
                       find-keywords)]
    (log/info (str (count counted-keywords) " keywords have been generated"))
    new-keywords))

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
         new-keywords (new-keywords-from-props highlights models :find-keywords)]
     new-keywords)))

(defn generate-keywords-for-aggregate
  [aggregate-name]
  (let [models (get-entity-manager (:database env))
        {status-model        :status
         timely-status-model :timely-status} models
        timely-statuses (find-timely-statuses-by-aggregate aggregate-name timely-status-model status-model)
        new-keywords (new-keywords-from-props timely-statuses models)]
    new-keywords))