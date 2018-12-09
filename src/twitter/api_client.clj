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

(def ^:dynamic *api-client-enabled-logging* false)

(def current-consumer-key (atom nil))
(def next-token (atom nil))

(def call-limits (atom {}))
(def endpoint-exclusion (atom {}))
(def frozen-tokens (atom {}))
(def rate-limits (atom {}))
(def remaining-calls (atom {}))

(def error-rate-limit-exceeded "Twitter responded to request with error 88: Rate limit exceeded.")
(def error-user-not-found "Twitter responded to request with error 50: User not found.")
(def error-user-suspended "Twitter responded to request with error 63: User has been suspended.")
(def error-not-authorized "Twitter responded to request '/1.1/friends/ids.json' with error 401: Not authorized.")
(def error-no-status "Twitter responded to request with error 144: No status found with that ID.")

; @see https://clojuredocs.org/clojure.core/declare about making forward declaration
(declare find-next-token)

(defn twitter-credentials
  "Make Twitter OAuth credentials from the environment configuration"
  ; @see https://github.com/adamwynne/twitter-api#restful-calls
  [token]
  (let [{consumer-key :consumer-key
         consumer-secret :consumer-secret
         token :token
         secret :secret} token]
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
    (when *api-client-enabled-logging*
      (log/info (str "The next consumer key issued from " context " is about to be \"" (:consumer-key token) "\"")))
    @next-token))

(defn consumer-keys-of-frozen-tokens
  "Return consumer keys of tokens which are frozen."
  []
  (let [now (l/local-now)]
  (if (nil? @frozen-tokens)
    '("_")
    (map name (keys (filter #(t/before? now (second %)) @frozen-tokens))))))

(defn wait-for-15-minutes
  [endpoint]
  (log/info (str "About to wait for 15 min so that the API is available again for \"" endpoint "\"" ))
  (Thread/sleep (* 60 15 1000)))

(defn select-token
  [endpoint token-candidate context model]
  (if (and
        (nil? token-candidate)
        (nil? @next-token))
    (do
      (wait-for-15-minutes endpoint)
      (find-next-token model endpoint context))
    (if (nil? token-candidate) @next-token token-candidate)))

(defn find-first-available-token-when
  [endpoint context model]
    (let [excluded-consumer-key @current-consumer-key
          excluded-consumer-keys (consumer-keys-of-frozen-tokens)
          token-candidate (find-first-available-tokens-other-than excluded-consumer-keys model)
          selected-token (select-token endpoint token-candidate context model)]
    (when *api-client-enabled-logging*
      (log/info (str "About to replace consumer key \"" excluded-consumer-key "\" with \""
                   (:consumer-key selected-token) "\" when " context)))
      selected-token))

(defn format-date
  [date]
  (let [built-in-formatter (f/formatters :basic-date-time)]
    (f/unparse built-in-formatter date)))

(defn is-token-candidate-frozen
  [token]
  (let [unfrozen-at (get @frozen-tokens (keyword (:consumer-key token)))
        now (l/local-now)
        formatted-now (format-date now)
        it-is-not (and
                    token
                    (or
                      (nil? unfrozen-at)
                      (t/after? now unfrozen-at)))]
    (when (and
            *api-client-enabled-logging*
            (not (nil? unfrozen-at)))
      (log/info (str "Now being \"" formatted-now "\" \""
                     (:consumer-key token) "\" will be unfrozen at \"" (format-date unfrozen-at) "\"")))
    (not it-is-not)))

(defn in-15-minutes
  []
  (c/from-long (+ (* 60 15 1000) (c/to-long (l/local-now)))))

(defn freeze-current-token
  []
  (let [later (in-15-minutes)]
    (swap! frozen-tokens #(assoc % (keyword @current-consumer-key) later))
    (when *api-client-enabled-logging*
      (log/info (str "\"" @current-consumer-key "\" should be available again at \"" (format-date later))))))

(defn handle-rate-limit-exceeded-error
  [endpoint token-model]
  (freeze-current-token)
  (freeze-token @current-consumer-key)
  (find-next-token token-model endpoint (str "a rate limited call to \"" endpoint "\""))
  (when (nil? @next-token)
    (wait-for-15-minutes endpoint)))

(defn try-calling-api
  [call endpoint token-model context]
  (let [excluded-until (get @endpoint-exclusion endpoint)
        now (l/local-now)]
    (when (or
            (nil? excluded-until)
            (t/after? now excluded-until))
              (try (with-open [client (ac/create-client)]
                     (call client))
                (catch Exception e
                  (log/warn (.getMessage e))
                  (cond
                    (= (.getMessage e) error-not-authorized)
                      (throw (Exception. (str error-not-authorized)))
                    (= endpoint "application/rate-limit-status")
                      (do
                        (swap! endpoint-exclusion #(assoc % endpoint (in-15-minutes)))
                        (when (string/includes? (.getMessage e) error-rate-limit-exceeded)
                          (handle-rate-limit-exceeded-error endpoint token-model)
                          (try-calling-api call endpoint token-model context)))))))))

(defn get-rate-limit-status
  [model]
  (let [resources "resources=favorites,statuses,users,lists,friends,friendships,followers"
        twitter-token (twitter-credentials @next-token)
        response (try-calling-api
                   #(application-rate-limit-status :client %
                                                   :oauth-creds twitter-token
                                                   :params {:resources resources})
                  "application/rate-limit-status"
                  model
                  "a call to \"application/rate-limit-status\"")
        resources (:resources (:body response))]
        (swap! rate-limits (constantly resources))))

(defn find-next-token
  [token-model endpoint context]
  (let [next-token-candidate (if @next-token
                               @next-token
                               (find-first-available-tokens token-model))
        it-is-frozen (is-token-candidate-frozen next-token-candidate)]
    (if it-is-frozen
      (do
        (set-next-token
          (find-first-available-token-when endpoint context token-model)
          context)
        (swap! remaining-calls #(assoc % (keyword endpoint) ((keyword endpoint)
                                                              @call-limits))))
      (set-next-token next-token-candidate context))))

(defn how-many-remaining-calls-for
  [endpoint token-model]
  (when (nil? (@remaining-calls (keyword endpoint)))
    (do
      (get-rate-limit-status token-model)
      (swap! remaining-calls #(assoc % (keyword endpoint) (:limit (get (:users @rate-limits) (keyword "/users/show/:id")))))))
  (get @remaining-calls (keyword endpoint)))

(defn how-many-remaining-calls-for-statuses
  [token-model]
  (when (nil? (@remaining-calls (keyword "statuses/show/:id" )))
    (do
      (get-rate-limit-status token-model)
      (swap! remaining-calls #(assoc
                                %
                                (keyword "statuses/show/:id")
                                (:limit (get (:statuses @rate-limits) (keyword "/statuses/show/:id")))))))
  (get @remaining-calls (keyword "statuses/show/:id" )))

(defn how-many-remaining-calls-showing-user
  [token-model]
  (how-many-remaining-calls-for "users/show" token-model))

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
  "Log remaining calls to endpoint by extracting limit and remaining values from HTTP headers"
  [headers endpoint]
  (let [are-headers-available (not (nil? headers))
        limit (when are-headers-available (:x-rate-limit-remaining headers))
        remaining-calls (if are-headers-available (:x-rate-limit-limit headers) 0)]

    (when *api-client-enabled-logging*
      (when limit
        (log/info (str "Rate limit at " limit " for \"" endpoint "\"" )))
      (log/info (str remaining-calls " remaining calls for \"" endpoint
                 "\" called with consumer key \"" @current-consumer-key "\"" )))

    (update-remaining-calls headers endpoint)))

(defn ten-percent-of
  [dividend]
  (* dividend 0.10))

(defn ten-percent-of-limit
  "Calculate 10 % of the rate limit, return 1 on exception"
  [headers]
  (def percentage (atom 1))
  (try (swap! percentage (constantly (ten-percent-of (Long/parseLong (:x-rate-limit-limit headers)))))
      (catch Exception e (log/error (str "An error occurred when calculating "
                                         "the 10 percent of the rate limit")))
      (finally @percentage)))

(defn guard-against-api-rate-limit
  "Wait for 15 min whenever a API rate limit is about to be reached"
  [headers endpoint & [on-reached-api-limit tokens]]
  (let [unavailable-rate-limit (nil? headers)
        percentage (ten-percent-of-limit headers)]
    (log-remaining-calls-for headers endpoint)
    (try
      (when
        (or
          unavailable-rate-limit
          (and
            (< (Long/parseLong (:x-rate-limit-remaining headers)) percentage)
            (nil? @next-token)))
        (when
          (fn? on-reached-api-limit)
            (on-reached-api-limit))
        (if (some? tokens)
          (handle-rate-limit-exceeded-error "statuses/show/:id" tokens)
          (wait-for-15-minutes endpoint)))
       (catch Exception e (log/error (.getMessage e))))))

(defn guard-against-exceptional-member
  [member model]
  (if (or
        (= 1 (:is-not-found member))
        (= 1 (:is-protected member))
        (= 1 (:is-suspended member)))
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
            (do
              (handle-rate-limit-exceeded-error "users/show" token-model)
              (get-twitter-user-by-id-or-screen-name {screen-name :screen-name id :id} token-model member-model))
          (string/includes? (.getMessage e) error-user-not-found)
            (guard-against-exceptional-member {:screen_name screen-name
                                               :twitter-id id
                                               :is-not-found 1
                                               :is-protected 0
                                               :is-suspended 0
                                               :total-subscribees 0
                                               :total-subscriptions 0} member-model)
          (string/includes? (.getMessage e) error-user-suspended)
            (guard-against-exceptional-member {:screen_name screen-name
                                               :twitter-id id
                                               :is-not-found 0
                                               :is-protected 0
                                               :is-suspended 1
                                               :total-subscribees 0
                                               :total-subscriptions 0} member-model))))))

(defn get-twitter-status-by-id
  [status-id model]
  (do
    (try
      (let [response (with-open [client (ac/create-client)]
                       (statuses-show-id
                         :client client
                         :oauth-creds (twitter-credentials @next-token)
                         :params {:id status-id}))]
        (update-remaining-calls (:headers response) "statuses/show/:id")
        (log/info (str "Fetched status having id #" status-id))
        response)
      (catch Exception e
        (log/warn (.getMessage e))
        (cond
          (string/includes? (.getMessage e) error-rate-limit-exceeded)
            (do
              (handle-rate-limit-exceeded-error "statuses/show/:id" model)
              (get-twitter-status-by-id status-id model))
          (string/includes? (.getMessage e) error-no-status)
            {:error error-no-status}
          :else
            (log/error (.getMessage e)))))))

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

(defn status-by-prop
  [status-id token-model context]
    (if
      (and
        (know-all-about-remaining-calls-and-limit)
        (is-rate-limit-exceeded))
      (do
        (freeze-current-token)
        (find-next-token token-model "statuses/show/:id" context)
        (status-by-prop status-id token-model context))
      (let [twitter-status (get-twitter-status-by-id status-id token-model)]
        twitter-status)))

(defn get-member-by-screen-name
  [screen-name token-model member-model]
  (let [_ (find-next-token token-model "users/show" "trying to call \"users/show\" with a screen name")
        user (member-by-prop {:screen-name screen-name} token-model member-model "a call to \"users/show\" with a screen name")
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show")
    (:body user)))

(defn get-member-by-id
  [id token-model member-model]
  (let [_ (find-next-token token-model "users/show" "trying to call \"users/show\" with an id")
        user (member-by-prop {:id id} token-model member-model "a call to \"users/show\" with an id")
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show")
    (:body user)))

(defn get-status-by-id
  [{id :id
    status-id :status-id} token-model]
  (let [status (status-by-prop status-id token-model "a call to \"statuses/show\" with an id")
        headers (:headers status)]
    (if
      (and
        (some? headers)
        (nil? (:error status)))
      (do
        (guard-against-api-rate-limit headers "statuses/show/:id" nil token-model)
        (assoc (:body status) :id id))
      nil)))

(defn get-id-of-member-having-username
  [screen-name member-model token-model]
  (let [matching-members (find-member-by-screen-name screen-name member-model)
        member (if matching-members
                 (first matching-members)
                 (get-member-by-screen-name screen-name token-model member-model))]
    {:twitter-id (:twitter-id member)
     :id (:id member)}))

(defn get-subscriptions-by-screen-name
  [screen-name tokens-model]
  (try-calling-api
    #(friends-ids :client %
                  :oauth-creds (twitter-credentials @next-token)
                  :params {:screen-name screen-name})
    "friends/id"
    tokens-model
    "a call to \"friends/id\""))

(defn get-subscribers-by-screen-name
  [screen-name tokens-model]
  (try-calling-api
    #(followers-ids :client %
                    :oauth-creds (twitter-credentials @next-token)
                    :params {:screen-name screen-name})
    "followers/id"
    tokens-model
    "a call to \"followers/id\""))

(defn get-subscriptions-of-member
  [screen-name token-model on-reached-api-limit]
  (let [_ (find-next-token token-model "users/show" "trying to make a call to \"friends/ids\"")
        subscriptions (get-subscriptions-by-screen-name screen-name token-model)
        headers (:headers subscriptions)
        friends (:body subscriptions)]
    (guard-against-api-rate-limit headers "friends/ids" on-reached-api-limit)
    (:ids friends)))

(defn get-subscribees-of-member
  [screen-name token-model]
  (let [_ (find-next-token token-model "users/show" "trying to make a call to \"followers/ids\"")
        subscribees (get-subscribers-by-screen-name screen-name token-model)
        headers (:headers subscribees)
        followers (:body subscribees)]
    (guard-against-api-rate-limit headers "followers/ids")
    (:ids followers)))

(defn get-favorites-by-screen-name
  [opts endpoint context tokens-model]
  (let [base-params {:count 200
                    :include-entities 1
                    :include-rts 1
                    :exclude-replies 0
                    :screen-name (:screen-name opts)
                    :trim-user 0
                    :tweet-mode "extended"}
        params (cond
          (not (nil? (:since-id opts)))
            (assoc base-params :since-id (:since-id opts))
          (not (nil? (:max-id opts)))
            (assoc base-params :max-id (:max-id opts))
          :else
            base-params)]

  (try-calling-api
    #(favorites-list
       :client %
       :oauth-creds (twitter-credentials @next-token)
       :params params)
    endpoint
    tokens-model
    (str "a " context))))

(defn get-favorites-of-member
  [opts token-model]
  (let [endpoint "favorites/list"
        call "call to \"favorites/list\""
        context (str "trying to make a " call)
        _ (find-next-token token-model endpoint context)
        response (get-favorites-by-screen-name opts endpoint call token-model)
        headers (:headers response)
        favorites (:body response)]
    (guard-against-api-rate-limit headers endpoint)
    favorites))

(defn get-statuses-by-screen-name
  [opts endpoint context tokens-model]
  (let [base-params {:count 200
                    :include-entities 1
                    :include-rts 1
                    :exclude-replies 0
                    :screen-name (:screen-name opts)
                    :trim-user 0
                    :tweet-mode "extended"}
        params (cond
          (not (nil? (:since-id opts)))
            (assoc base-params :since-id (:since-id opts))
          (not (nil? (:max-id opts)))
            (assoc base-params :max-id (:max-id opts))
          :else
            base-params)]

  (try-calling-api
    #(statuses-user-timeline :client %
                    :oauth-creds (twitter-credentials @next-token)
                    :params params)
    endpoint
    tokens-model
    (str "a " context))))

(defn get-statuses-of-member
  [opts token-model]
  (let [endpoint "statuses/user_timeline"
        call "call to \"statuses/user_timeline\""
        context (str "trying to make a " call)
        _ (find-next-token token-model endpoint context)
        response (get-statuses-by-screen-name opts endpoint call token-model)
        headers (:headers response)
        favorites (:body response)]
    (guard-against-api-rate-limit headers endpoint)
    favorites))
