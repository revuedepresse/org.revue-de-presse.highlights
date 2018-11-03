(ns twitter.api-client
    (:require [clojure.edn :as edn]
              [environ.core :refer [env]])
    (:use [twitter.oauth]
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
  (users-show :oauth-creds (twitter-credentials)
              :params {:screen-name screen-name}))
