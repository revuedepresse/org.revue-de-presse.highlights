(ns twitter.api-client
    (:require [clojure.edn :as edn]
              [clojure.string :as string]
              [clojure.tools.logging :as log]
              [environ.core :refer [env]]
              [http.async.client :as ac]
              [clj-time.format :as f]
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

(def call-limits (atom {}))
(def frozen-tokens (atom {}))
(def rate-limits (atom {}))
(def remaining-calls (atom {}))

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
    (log/info (str "The next consumer key issued from " context " is about to be \"" (:consumer-key token) "\""))
    @next-token))

(defn try-calling-api
  [call token-model context]
    (try (call)
      (catch Exception e
        (log/warn (.getMessage e))
        (string/includes? (.getMessage e) error-rate-limit-exceeded)
        (freeze-token @current-consumer-key token-model)
        (set-next-token
          (find-first-available-tokens-other-than @current-consumer-key token-model " when trying to call API")
          context)
        (try-calling-api call token-model context))))

(defn get-rate-limit-status
  [model]
  (let [resources "resources=favorites,statuses,users,lists,friends,friendships,followers"
        twitter-token (twitter-credentials @next-token)
        response (try-calling-api #(with-open [client (ac/create-client)]
                  (application-rate-limit-status :client client
                                                 :oauth-creds twitter-token
                                                 :params {:resources resources}))
                                  model
                                  "a call to \"application/rate-limit-status\"")
        resources (:resources (:body response))]
        (swap! rate-limits (constantly resources))))

(defn how-many-remaining-calls-for
  [endpoint token-model]
  (when (nil? (@remaining-calls (keyword endpoint)))
    (do
      (get-rate-limit-status token-model)
      (swap! remaining-calls #(assoc % (keyword endpoint) (:limit (get (:users @rate-limits) (keyword "/users/show/:id")))))))
  (get @remaining-calls (keyword endpoint)))

(defn how-many-remaining-calls-showing-user
  [token-model]
  (how-many-remaining-calls-for "users/show" token-model))

(defn is-token-candidate-frozen
  [token]
  (let [unfrozen-at (get @frozen-tokens (keyword (:consumer-key token)))
        now (l/local-now)
        it-is-not (and
                    token
                    (or
                      (nil? unfrozen-at)
                      (t/after? now unfrozen-at)))]
    (not it-is-not)))

(defn find-next-token
  [token-model endpoint context]
  (let [next-token-candidate (if @next-token (find-first-available-tokens token-model))
        it-is-frozen (is-token-candidate-frozen next-token-candidate)]
  (if it-is-frozen
    (do
      (set-next-token (find-first-available-tokens-other-than @current-consumer-key token-model context) context)
      (swap! remaining-calls #(assoc % (keyword endpoint) ((keyword endpoint) @call-limits))))
    (set-next-token next-token-candidate context))))

(defn update-remaining-calls
  [headers endpoint]
  (let [endpoint-keyword (keyword endpoint)]
  (when
    (and
      (pos? (count headers))
      (not (nil? (:x-rate-limit-remaining headers))))
    (try
      (swap! remaining-calls #(assoc % endpoint-keyword (Long/parseLong (:x-rate-limit-remaining headers))))
      (catch Exception e (log/warn (.getMessage e))))
    (when
      (and
        (nil? (get @call-limits endpoint-keyword))
        (:x-rate-limit-limit headers))
      (swap! call-limits #(assoc % endpoint-keyword (Long/parseLong (:x-rate-limit-limit headers))))))))

(defn log-remaining-calls-for
  [headers endpoint]
  (log/info (str "Rate limit at " (:x-rate-limit-limit headers) " for \"" endpoint "\"" ))
  (log/info (str (:x-rate-limit-remaining headers) " remaining calls for \"" endpoint
                 "\" called with consumer key \"" @current-consumer-key "\"" ))
  (update-remaining-calls headers endpoint))

(defn ten-percent-of
  [dividend]
  (* dividend 0.95))

(defn ten-percent-of-limit
  "Calculate 10 % of the rate limit, return 1 on exception"
  [headers]
  (def percentage (atom 1))
  (try (swap! percentage (constantly (ten-percent-of (Long/parseLong (:x-rate-limit-limit headers)))))
      (catch Exception e (log/error (.getMessage e)))
      (finally @percentage)))

(defn guard-against-api-rate-limit
  "Wait for 15 min whenever a API rate limit is about to be reached"
  [headers endpoint]
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
  (let [response (with-open [client (ac/create-client)]
                  (users-show :client client :oauth-creds (twitter-credentials @next-token)
                              :params {:screen-name screen-name}))]
    (update-remaining-calls (:headers response) "users/show")
    response))

(defn get-twitter-user-by-id
  [id]
  (let [response (with-open [client (ac/create-client)]
                  (users-show :client client :oauth-creds (twitter-credentials @next-token)
                              :params {:id id}))]
    (update-remaining-calls (:headers response) "users/show")
    response))

(defn in-15-minutes
  []
  (c/from-long (+ (* 60 15 1000) (c/to-long (l/local-now)))))

(defn freeze-current-token
  []
  (let [built-in-formatter (f/formatters :basic-date-time)
        later (in-15-minutes)]
  (swap! frozen-tokens #(assoc % (keyword @current-consumer-key) later))
  (log/info (str "\"" @current-consumer-key "\" should be available again at \"" (f/unparse built-in-formatter later)))))

(defn get-twitter-user-by-id-or-screen-name
  [{screen-name :screen-name id :id} token-model member-model]
  (do
    (try
      (if (nil? id)
        (get-twitter-user-by-screen-name screen-name)
        (get-twitter-user-by-id id))
      (catch Exception e
        (log/warn (.getMessage e))
        (cond
          (string/includes? (.getMessage e) error-rate-limit-exceeded)
          (freeze-token @current-consumer-key token-model)
          (string/includes? (.getMessage e) error-user-not-found)
          (guard-against-exceptional-member {:screen_name screen-name :is-not-found 1} member-model))))))

(defn know-all-about-remaining-calls-and-limit
  []
  (and
    (:users/show @remaining-calls)
    (:users/show @call-limits)))

(defn is-rate-limit-exceeded
  []
  (let [ten-percent-of-call-limits (ten-percent-of (:users/show @call-limits))]
    (<= (:users/show @remaining-calls) ten-percent-of-call-limits)))

(defn member-by-prop
  [member token-model member-model context]
    (if
      (and
        (know-all-about-remaining-calls-and-limit)
        (is-rate-limit-exceeded))
      (do
        (freeze-current-token)
        (find-next-token token-model "users/show" context)
        (member-by-prop member token-model member-model context))
      (let [twitter-user (get-twitter-user-by-id-or-screen-name member token-model member-model)]
        (if (nil? twitter-user)
          (do
            (find-next-token token-model "users/show" context)
            (member-by-prop member token-model member-model context))
          twitter-user))))

(defn get-member-by-screen-name
  [screen-name token-model member-model]
  (let [_ (find-next-token token-model "users/show" "a call to \"users/show\" with a screen name")
        user (member-by-prop {:screen-name screen-name} token-model member-model "a call to \"users/show\" with a screen name")
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show")
    (:body user)))

(defn get-member-by-id
  [id token-model member-model]
  (let [_ (find-next-token token-model "users/show" "a call to \"users/show\" with an id")
        user (member-by-prop {:id id} token-model member-model "a call to \"users/show\" with an id")
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show")
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
  (let [_ (find-next-token token-model "users/show" "a call to \"friends/ids\"")
        subscriptions (get-subscriptions-by-screen-name screen-name)
        headers (:headers subscriptions)
        friends (:body subscriptions)]
    (guard-against-api-rate-limit headers "friends/ids")
    (:ids friends)))

(defn get-subscribees-of-member
  [screen-name token-model]
  (let [_ (find-next-token token-model "users/show" "a call to \"followers/ids\"")
        subscribees (get-subscribers-by-screen-name screen-name)
        headers (:headers subscribees)
        followers (:body subscribees)]
    (guard-against-api-rate-limit headers "followers/ids")
    (:ids followers)))