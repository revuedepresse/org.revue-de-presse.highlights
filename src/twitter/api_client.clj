(ns twitter.api-client
    (:require [clojure.edn :as edn]
              [clojure.string :as string]
              [clojure.tools.logging :as log]
              [environ.core :refer [env]]
              [http.async.client :as ac]
              [clj-time.core :as t]
              [clj-time.local :as l]
              [clj-time.coerce :as c])
    (:use [repository.entity-manager]
          [twitter.oauth]
          [twitter.callbacks]
          [twitter.callbacks.handlers]
          [twitter.api.restful]))

(def current-consumer-key (atom nil))
(def next-token (atom nil))

(def remaining-calls (atom {}))
(def call-limits (atom {}))
(def frozen-tokens (atom {}))

(def error-rate-limit-exceeded "Twitter responded to request with error 88: Rate limit exceeded.")
(def error-user-not-found "Twitter responded to request with error 50: User not found.")

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

(defn set-next-token
  "Set the next token by swapping the value of an atom
  and declare the current consumer key related to this token"
  [token context]
  (let [token-candidate token
        consumer-key (:consumer-key token)]
    (swap! next-token (constantly token-candidate))         ; @see https://clojuredocs.org/clojure.core/constantly
    (swap! current-consumer-key (constantly consumer-key))
    (log/info "The next consumer key issued from " context " is about to be \""(:consumer-key token)"\"")
    @next-token))

(defn find-next-token
  [token-model context]
  (let [next-token-candidate (find-first-available-tokens token-model)
        unfrozen-at (get @frozen-tokens (keyword (:consumer-key next-token-candidate)))]
  (if (and
        next-token-candidate
        (or
          (nil? unfrozen-at)
          (t/after? (l/local-now) unfrozen-at)))
    (set-next-token next-token-candidate context)
    (set-next-token (find-first-available-tokens-other-than @current-consumer-key token-model) context))))

(defn log-remaining-calls-for
  [headers endpoint]
  (log/info (str "Rate limit at " (:x-rate-limit-limit headers) " for \"" endpoint "\"" ))
  (log/info (str (:x-rate-limit-remaining headers) " remaining calls for \"" endpoint "\"" ))
  (when (not (nil? (:x-rate-limit-remaining headers)))
    (swap! remaining-calls #(assoc % (keyword endpoint) (Long/parseLong (:x-rate-limit-remaining headers)))))
  (when (:x-rate-limit-limit headers)
    (swap! call-limits #(assoc % (keyword endpoint) (Long/parseLong (:x-rate-limit-limit headers))))))

(defn ten-percent-of
  [dividend]
  (/ dividend 10))

(defn ten-percent-of-limit
  "Calculate 10 % of the rate limit, return 1 on exception"
  [headers]
  (def percentage (atom 1))
  (try (swap! percentage (constantly (ten-percent-of (Long/parseLong (:x-rate-limit-limit headers)))))
      (catch Exception e (log/error (.getMessage e)))
      (finally @percentage)))

(defn guard-against-api-rate-limit
  "Wait for 15 min whenever a API rate limit is about to be reached"
  [headers endpoint model]
  (let [percentage (ten-percent-of-limit headers)]
    (log-remaining-calls-for headers endpoint)
    (try (when (and
            (< (Long/parseLong (:x-rate-limit-remaining headers)) percentage)
            (nil? @next-token))
            (log/info (str "About to wait for 15 min so that the API is available again for \"" endpoint "\"" ))
            (Thread/sleep (* 60 15 1000)))
       (catch Exception e (log/error (.getMessage e))))))

(defn guard-against-exceptional-member
  [member model]
  (if (or (:is-not-found member)
          (:is-protected member)
          (:is-suspended member))
    (new-member member model)
    member))

(defn get-twitter-user-by-screen-name
  [screen-name]
  (with-open [client (ac/create-client)]
    (users-show :client client :oauth-creds (twitter-credentials @next-token)
                :params {:screen-name screen-name})))

(defn get-twitter-user-by-id
  [id]
  (with-open [client (ac/create-client)]
    (users-show :client client :oauth-creds (twitter-credentials @next-token)
                :params {:id id})))

(defn in-15-minutes
  []
  (c/from-long (+ (* 60 15 000) (c/to-long (l/local-now)))))

(defn member-by-prop
  [{screen-name :screen-name id :id}  token-model member-model context]

  (def member (atom nil))

  (when (and (:users/show @remaining-calls)
             (:users/show @call-limits)
             (<= (:users/show @remaining-calls) (ten-percent-of (:users/show @call-limits))))
    (swap! frozen-tokens #(assoc % (keyword @current-consumer-key) (in-15-minutes)))
    (set-next-token (find-first-available-tokens-other-than @current-consumer-key token-model) context))

  (try
    (if (nil? id)
        (swap! member (constantly (get-twitter-user-by-screen-name screen-name)))
        (swap! member (constantly (get-twitter-user-by-id id))))
    (catch Exception e
      (log/warn (.getMessage e))
      (cond
        (string/includes? (.getMessage e) error-rate-limit-exceeded)
          (freeze-token @current-consumer-key token-model)
        (string/includes? (.getMessage e) error-user-not-found)
          (swap! member (constantly {:screen_name screen-name
                                   :is-not-found 1})))))
  (if (nil? (deref member))
    (do
      (set-next-token (find-first-available-tokens-other-than @current-consumer-key token-model) context)
      (member-by-prop {:screen-name screen-name :id id} token-model member-model context))
    (guard-against-exceptional-member @member member-model)))

(defn get-member-by-screen-name
  [screen-name token-model member-model]
  (let [_ (find-next-token token-model "a call to \"users/show\" with a screen name")
        user (member-by-prop {:screen-name screen-name} token-model member-model "a call to \"users/show\" with a screen name")
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show" token-model)
    (:body user)))

(defn get-member-by-id
  [id token-model member-model]
  (let [_ (find-next-token token-model "a call to \"users/show\" with an id")
        user (member-by-prop {:id id} token-model member-model "a call to \"users/show\" with an id")
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show" token-model)
    (:body user)))

(defn get-id-of-member-having-username
  [screen-name member-model token-model]
  (let [matching-members (find-member-by-screen-name screen-name member-model)
        member (if matching-members
                  (first matching-members)
                  (get-member-by-screen-name screen-name token-model member-model))]
    (:id member)))

(defn get-subscriptions-by-screen-name
  [screen-name]
  (with-open [client (ac/create-client)]
    (friends-ids
      :client client
      :oauth-creds (twitter-credentials @next-token)
      :params {:screen-name screen-name})))

(defn get-subscribers-by-screen-name
  [screen-name]
  (with-open [client (ac/create-client)]
    (followers-ids
      :client client
      :oauth-creds (twitter-credentials @next-token)
      :params {:screen-name screen-name})))

(defn get-subscriptions-of-member
  [screen-name token-model]
  (let [_ (find-next-token token-model "a call to \"friends/ids\"")
        subscriptions (get-subscriptions-by-screen-name screen-name)
        headers (:headers subscriptions)
        friends (:body subscriptions)]
    (guard-against-api-rate-limit headers "friends/ids" token-model)
    (:ids friends)))

(defn get-subscribees-of-member
  [screen-name token-model]
  (let [_ (find-next-token token-model "a call to \"followers/ids\"")
        subscribees (get-subscribers-by-screen-name screen-name)
        headers (:headers subscribees)
        followers (:body subscribees)]
    (guard-against-api-rate-limit headers "followers/ids" token-model)
    (:ids followers)))