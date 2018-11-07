(ns twitter.api-client
    (:require [clojure.edn :as edn]
              [clojure.tools.logging :as log]
              [environ.core :refer [env]])
    (:use [repository.entity-manager]
          [twitter.oauth]
          [twitter.callbacks]
          [twitter.callbacks.handlers]
          [twitter.api.restful]))

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
  (let [users (users-show :oauth-creds (twitter-credentials)
              :params {:screen-name screen-name})]
    users))


(defn get-member-by-id
  [id]
  (users-show :oauth-creds (twitter-credentials)
              :params {:id id}))

(defn get-id-of-member-having-username
  [screen-name members]
  (let [matching-members (find-member-by-screen-name screen-name members)
        member (if matching-members
                  (first matching-members)
                  (get-member-by-screen-name screen-name))]
    (:id member)))

(defn get-subscriptions-of-member
  [screen-name]
  (let [subscriptions (friends-ids
                      :oauth-creds (twitter-credentials)
                      :params {:screen-name screen-name})
        headers (:headers subscriptions)
        friends (:body subscriptions)]
    (log/info (str "Rate limit at " (:x-rate-limit-limit headers) " for \"friends/ids\"" ))
    (log/info (str (:x-rate-limit-remaining headers) " remaining calls for \"friends/ids\"" ))
    (:ids friends)))

(defn get-subscribees-of-member
  [screen-name]
  (let [subscribees (followers-ids
                        :oauth-creds (twitter-credentials)
                        :params {:screen-name screen-name})
        headers (:headers subscribees)
        followers (:body subscribees)]
    (log/info (str "Rate limit at " (:x-rate-limit-limit headers) " for \"followers/ids\"" ))
    (log/info (str (:x-rate-limit-remaining headers) " remaining calls for \"followers/ids\"" ))
    (:ids followers)))