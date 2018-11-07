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
  [token]
  (let [{consumer-key :consumer-key
         consumer-secret :consumer-secret
         token :token
         secret :secret} token
        credentials (edn/read-string (:twitter env))]
    (make-oauth-creds consumer-key
                      consumer-secret
                      token
                      secret)))

(defn log-remaining-calls-for
  [headers endpoint]
  (log/info (str "Rate limit at " (:x-rate-limit-limit headers) " for \"" endpoint "\"" ))
  (log/info (str (:x-rate-limit-remaining headers) " remaining calls for \"" endpoint "\"" )))

(defn guard-against-api-rate-limit
  "Wait for 15 min whenever a API rate limit is about to be reached"
  [headers endpoint]
  (let [ten-percent-of-limit (/ (Long/parseLong (:x-rate-limit-limit headers)) 10)]
    (log-remaining-calls-for headers endpoint)
    (when
      (< (Long/parseLong (:x-rate-limit-remaining headers)) ten-percent-of-limit)
      (log/info (str "About to wait for 15 min so that the API is available again for \"" endpoint "\"" ))
      (Thread/sleep (* 60 15 1000)))))

(defn get-member-by-screen-name
  [screen-name token-model]
  (let [token (find-first-available-tokens token-model)
        user (users-show :oauth-creds (twitter-credentials token)
              :params {:screen-name screen-name})
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show")
    (:body user)))

(defn get-member-by-id
  [id token-model]
  (let [token (find-first-available-tokens token-model)
        user (users-show :oauth-creds (twitter-credentials token)
              :params {:id id})
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show")
    (:body user)))

(defn get-id-of-member-having-username
  [screen-name member-model token-model]
  (let [matching-members (find-member-by-screen-name screen-name member-model)
        member (if matching-members
                  (first matching-members)
                  (get-member-by-screen-name screen-name token-model))]
    (:id member)))

(defn get-subscriptions-of-member
  [screen-name token-model]
  (let [token (find-first-available-tokens token-model)
        subscriptions (friends-ids
                      :oauth-creds (twitter-credentials token)
                      :params {:screen-name screen-name})
        headers (:headers subscriptions)
        friends (:body subscriptions)]
    (guard-against-api-rate-limit headers "friends/ids")
    (:ids friends)))

(defn get-subscribees-of-member
  [screen-name token-model]
  (let [token (find-first-available-tokens token-model)
        subscribees (followers-ids
                        :oauth-creds (twitter-credentials token)
                        :params {:screen-name screen-name})
        headers (:headers subscribees)
        followers (:body subscribees)]
    (guard-against-api-rate-limit headers "followers/ids")
    (:ids followers)))