(ns command.save-today-highlights
  (:require [environ.core :refer [env]]
            [clj-uuid :as uuid]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [repository.highlight]))

(defn extract-total-props
  [document]
  (let [api-document (:api-document document)
        decoded-document (json/read-str api-document)
        highlight-props {:id (uuid/to-string (uuid/v1))
                         :member-id (:member-id document)
                         :status-id (:status-id document)
                         :publication-date-time (:publication-date-time document)
                         :total-retweets (get decoded-document "retweet_count")
                         :total-favoritess (get decoded-document "favorite_count")}]
    (log/info (str "Prepared highlight for member #" (:member-id highlight)
                   " and status #" (:status-id highlight)))
  highlight-props))

(defn save-today-highlights
  []
  (let [{highlight-model :highlight
         status-model :status
         member-model :members} (get-entity-manager (:database env))
        press-aggregate-name (:press (edn/read-string (:aggregate env)))
        today-statuses (find-today-statuses-for-aggregate press-aggregate-name)
        documents (doall (map extract-total-props today-statuses))]
    ))
