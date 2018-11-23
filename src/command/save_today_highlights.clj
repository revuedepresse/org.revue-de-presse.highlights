(ns command.save-today-highlights
  (:require [environ.core :refer [env]]
            [clj-uuid :as uuid]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [repository.highlight]
        [twitter.status]))

(defn extract-total-props
  [document]
  (let [api-document (:api-document document)
        decoded-document (json/read-str api-document)
        highlight-props {:id (uuid/to-string (uuid/v1))
                         :member-id (:member-id document)
                         :status-id (:status-id document)
                         :publication-date-time (:publication-date-time document)
                         :total-retweets (get decoded-document "retweet_count")
                         :total-favorites (get decoded-document "favorite_count")}]
    (log/info (str "Prepared highlight for member #" (:member-id highlight-props)
                   " and status #" (:status-id highlight-props)))
  highlight-props))

(defn save-highlights
  ([]
    (save-highlights nil))
  ([date]
    (let [{highlight-model :highlight
           status-model :status
           member-model :members} (get-entity-manager (:database env))
          press-aggregate-name (:press (edn/read-string (:aggregate env)))
          statuses (find-statuses-for-aggregate press-aggregate-name date)
          find #(find-highlights-having-ids % highlight-model member-model status-model)
          filtered-statuses (filter-out-known-statuses find statuses)
          highlights-props (doall (map extract-total-props filtered-statuses))
          new-highlights (bulk-insert-new-highlights highlights-props highlight-model member-model status-model)]
      (log/info (str "There are " (count new-highlights) " new highlights")))))

(defn save-today-highlights
  []
  (save-highlights))
