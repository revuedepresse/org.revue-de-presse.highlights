(ns twitter.member
  (:require [clojure.tools.logging :as log])
  (:use [repository.entity-manager]
        [twitter.api-client]))

(def ^:dynamic *twitter-member-enabled-logging* false)

(defn get-ids-of-statuses-authors
  [statuses]
  (map (fn [{{member-id :id_str} :user}] member-id) statuses))

(defn get-missing-members-ids
  [statuses model]
  (let [ids (get-ids-of-statuses-authors statuses)
        found-members (find-members-having-ids ids model)
        matching-ids (set (map #(:twitter-id %) found-members))
        missing-ids (clojure.set/difference (set ids) (set matching-ids))]
    missing-ids))

(defn assoc-twitter-user-properties
  [twitter-user]
  {:description (:description twitter-user)
   :is-protected (if (not= (:protected twitter-user) "false") 1 0)
   :is-suspended 0
   :is-not-found 0
   :total-subscribees (:followers_count twitter-user)
   :total-subscriptions (:friends_count twitter-user)
   :twitter-id (:id_str twitter-user)
   :screen-name (:screen_name twitter-user)})

(defn save-member
  [twitter-user model & [only-props]]
  (let [props {:description (:description twitter-user)
               :is-protected (if (false? (:protected twitter-user)) 0 1)
               :is-suspended 0
               :is-not-found 0
               :url (:url twitter-user)
               :total-subscribees (if (nil? (:followers_count twitter-user))
                                    0
                                    (:followers_count twitter-user))
               :total-subscriptions (if (nil? (:friends_count twitter-user))
                                      0
                                      (:friends_count twitter-user))
               :twitter-id (:id_str twitter-user)
               :screen-name (:screen_name twitter-user)}]
    (if only-props
      props
      (new-member props model))))

(defn get-new-member-logger
  [member-type]
  (fn [member]
    (log/info (str "New " member-type " \"" (:screen-name member)
                   "\" having id #" (:twitter-id member) " has been cached."))))

(defn assoc-properties-of-twitter-users
  [twitter-users]
  (log/info "Build a sequence of twitter users properties")
  (doall (pmap assoc-twitter-user-properties twitter-users)))

(defn get-twitter-user
  [member-id tokens members]
  (log/info (str "About to look up for member having twitter id #" member-id))
  (let [twitter-user (get-member-by-id member-id tokens members)]
    twitter-user))

(defn ensure-authors-of-status-exist
  [statuses model token-model]
  (let [remaining-calls (how-many-remaining-calls-showing-user token-model)
        authors-ids (map #(:id (:user %)) statuses)
        total-authors (count authors-ids)]

    (if (pos? total-authors)
      (do
        (log/info (str "About to ensure " total-authors " member(s) exist."))
        (let [twitter-users (if
                              (and
                                (not (nil? remaining-calls))
                                (< total-authors remaining-calls))
                              (pmap #(get-twitter-user % token-model model) authors-ids)
                              (map #(get-twitter-user % token-model model) authors-ids))
              twitter-users-properties (assoc-properties-of-twitter-users twitter-users)
              deduplicated-users-properties (dedupe (sort-by #(:twitter-id %) twitter-users-properties))
              new-members (bulk-insert-new-members deduplicated-users-properties model)]
          (doall (map #(log/info (str "Member #" (:twitter-id %)
                                      " having screen name \"" (:screen-name %)
                                      "\" has been saved under id \"" (:id %))) new-members)))
        (log/info (str "No need to find some missing member."))))))

(defn new-member-from-json
  [member-id tokens members]
  (when *twitter-member-enabled-logging*
    (log/info (str "About to look up for member having twitter id #" member-id)))
  (let [twitter-user (get-member-by-id member-id tokens members)
        member (save-member twitter-user members)]
    member))

(defn new-member-props-from-json
  [member-id tokens members]
  (when *twitter-member-enabled-logging*
    (log/info (str "About to look up for member having twitter id #" member-id)))
  (let [twitter-user (get-member-by-id member-id tokens members)
        member (save-member twitter-user members :only-props)]
    member))

(defn ensure-members-exist
  [members-ids tokens members register-member]
  (let [remaining-calls (how-many-remaining-calls-showing-user tokens)
        total-members (count members-ids)]

    (if (pos? total-members)
      (log/info (str "About to ensure " total-members " member(s) exist."))
      (log/info (str "No need to find some missing member.")))

    (if (and
          (not (nil? remaining-calls))
          (< total-members remaining-calls))
      (doall (pmap #(register-member % tokens members) members-ids))
      (doall (map #(register-member % tokens members) members-ids)))))

(defn ensure-member-having-id-exists
  [twitter-id model token-model]
  (let [existing-member (find-member-by-twitter-id twitter-id model)
        member (if (pos? (count existing-member))
                 (first existing-member)
                 (do
                   (ensure-members-exist (list twitter-id) token-model model new-member-from-json)
                   (first (find-member-by-twitter-id twitter-id model))))]
    member))
