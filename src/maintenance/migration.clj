(ns maintenance.migration
  (:require [repository.publication :as publication]
            [repository.status :as status]
            [environ.core :refer [env]])
  (:use [repository.entity-manager]
        [pandect.algo.sha1 :refer :all]
        [pandect.algo.sha256 :refer :all]))

(defn status-to-publication
  [status]
  (let [hash (sha256 (str (:screen-name status) "|"
                          (:status-twitter-id status)))]
    {:legacy_id    (:id status)
     :hash         hash
     :text         (:text status)
     :screen_name  (:screen-name status)
     :avatar_url   (:avatar-url status)
     :document_id  (:status-twitter-id status)
     :document     (:document status)
     :published_at (:created-at status)}))

(defn migrate-status-to-publications
  [{status-model      :status
    publication-model :publication}]
  (let [status (status/find-unpublished-statuses status-model)
        publication-props (doall (pmap status-to-publication status))
        _ (publication/bulk-insert-of-publication-props
            publication-props
            publication-model
            status-model)]
    status))

(defn migrate-all-status-to-publications
  []
  (let [models (get-entity-manager (:database env))
        published-status (migrate-status-to-publications models)]
    (loop [published-status published-status]
      (when (> (count published-status) 0)
        (recur (migrate-status-to-publications models))))))