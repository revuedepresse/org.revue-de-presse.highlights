(ns command.update-members-props
  (:require [environ.core :refer [env]]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [repository.member]))

(defn extract-member-props
  [status]
  (if-not (nil? (:api-document status))
    (let [empty-props {:id (:id status)
                       :url ""
                       :screen-name nil
                       :description ""}
          json-decoded-document (json/read-str (:api-document status))
          user (get json-decoded-document "user")
          user-screen-name (get user "screen_name")
          description (get user "description")
          url (get user "url")
          props (if (= user-screen-name (:screen-name status))
            {:id (:id status)
             :url (if (nil? url) "" url)
             :screen-name user-screen-name
             :description (if (nil? description) "" description)}
            empty-props)]
      props)
      {:id (:id status)
       :url ""
       :screen-name nil
       :description ""}))

(defn get-member-modifier
  [model]
  (fn [member-props]
    (if (:screen-name member-props)
      (do
        (log/info (str "About to update description and URL of member \""
                     (:screen-name member-props)  "\""))
        (update-member-description-and-url member-props model))
      (log/info (str "Could not update description and URL of member #"
                     (:id member-props) " without original status document")))))

(defn update-members-page
  [page total-pages page-length model]
  (let [statuses (find-single-statuses-per-member (- total-pages page) page-length)
        members-props (doall (map extract-member-props statuses))
        filtered-members-props (filter #(not (nil? (:screen-name %))) members-props)
        deduplicated-members-props (dedupe (sort-by #(:id %) filtered-members-props))]
    (doall (pmap (get-member-modifier model) deduplicated-members-props))))

(defn update-members-descriptions-urls
  []
  (let [{member-model :members} (get-entity-manager (:database env))
        total-members (count-members)
        page-length 10000
        total-pages (inc (quot total-members page-length))]
    (loop [page total-pages]
      (when (and
              (not (nil? page))
              (pos? page))
        (update-members-page page total-pages page-length member-model)
        (recur (dec page))))))