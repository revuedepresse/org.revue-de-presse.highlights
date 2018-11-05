(ns twitter.api-client
    (:require [clojure.edn :as edn]
              [environ.core :refer [env]])
    (:use [repository.entity-manager]
          [twitter.oauth]
          [twitter.callbacks]
          [twitter.callbacks.handlers]
          [twitter.api.restful]))

(def ^:dynamic *skip-api-calls* false)

(defn twitter-credentials
  "Make Twitter OAuth credentials from the environment configuration"
  ; @see https://github.com/adamwynne/twitter-api#restful-calls
  [& args]
  (let [credentials (edn/read-string (:twitter env))]
    (make-oauth-creds (:consumer-key credentials)
                      (:consumer-secret credentials)
                      (:token credentials)
                      (:secret credentials))))

(defn get-member-by-screen-name
  [screen-name]
  (if *skip-api-calls*
    {}
    (users-show :oauth-creds (twitter-credentials)
              :params {:screen-name screen-name})))

(defn get-id-of-member-having-username
  [screen-name members]
  (let [matching-members (find-member-by-screen-name screen-name members)
        member (if matching-members
                  (first matching-members)
                  (get-member-by-screen-name screen-name))]
    (:id member)))

(defn get-subscriptions-of-member
  [screen-name]
  (if *skip-api-calls*
    {}
    (:ids (:body (friends-ids :oauth-creds (twitter-credentials)
               :params {:screen-name screen-name})))))

(defn get-subscribees-of-member
  [screen-name]
  (if *skip-api-calls*
    {}
    (:ids (:body (followers-ids :oauth-creds (twitter-credentials)
                 :params {:screen-name screen-name})))))