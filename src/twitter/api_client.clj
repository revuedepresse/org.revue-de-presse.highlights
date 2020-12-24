(ns twitter.api-client
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [http.async.client :as ac]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.local :as l]
            [clj-time.coerce :as c]
            [utils.error-handler :as error-handler])
  (:use [repository.entity-manager]
        [repository.member]
        [repository.token]
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

(def error-page-not-found "Twitter responded to request with error 34: Sorry, that page does not exist.")
(def error-rate-limit-exceeded "Twitter responded to request with error 88: Rate limit exceeded.")
(def error-user-not-found "Twitter responded to request with error 50: User not found.")
(def error-missing-status-id "https://api.twitter.com/1.1/statuses/show/{:id}.json needs :id param to be supplied")
(def error-user-suspended "Twitter responded to request with error 63: User has been suspended.")
(def error-unauthorized-user-timeline-statuses-access "Twitter responded to request '/1.1/statuses/user_timeline.json' with error 401: Not authorized.")
(def error-unauthorized-friends-ids-access "Twitter responded to request '/1.1/friends/ids.json' with error 401: Not authorized.")
(def error-unauthorized-favorites-list-access "Twitter responded to request '/1.1/favorites/list.json' with error 401: Not authorized.")
(def error-no-status "Twitter responded to request with error 144: No status found with that ID.")

; @see https://clojuredocs.org/clojure.core/declare about making forward declaration
(declare find-next-token)

(defn twitter-credentials
  "Make Twitter OAuth credentials from the environment configuration"
  ; @see https://github.com/adamwynne/twitter-api#restful-calls
  [token]
  (let [{consumer-key    :consumer-key
         consumer-secret :consumer-secret
         token           :token
         secret          :secret} token]
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
  (log/info (str "About to wait for 15 min so that the API is available again for \"" endpoint "\""))
  (Thread/sleep (* 60 15 1000)))

(defn find-token
  [endpoint token-candidate context model token-type-model]
  (if (and
        (nil? token-candidate)
        (nil? @next-token))
    (do
      (wait-for-15-minutes endpoint)
      (find-next-token model token-type-model endpoint context))
    (if (nil? token-candidate) @next-token token-candidate)))

(defn find-first-available-token-when
  [endpoint context token-model token-type-model]
  (let [excluded-consumer-key @current-consumer-key
        excluded-consumer-keys (consumer-keys-of-frozen-tokens)
        token-candidate (find-first-available-tokens-other-than excluded-consumer-keys token-model token-type-model)
        selected-token (find-token endpoint token-candidate context token-model token-type-model)]
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
  [endpoint token-model token-type-model]
  (freeze-current-token)
  (freeze-token @current-consumer-key)
  (find-next-token token-model token-type-model endpoint (str "a rate limited call to \"" endpoint "\""))
  (when (nil? @next-token)
    (wait-for-15-minutes endpoint)))

(defn try-calling-api
  [call endpoint token-model token-type-model context]
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
               (string/starts-with? (.getMessage e) error-rate-limit-exceeded) (throw (Exception. (str error-rate-limit-exceeded)))
               (= (.getMessage e) error-page-not-found) (throw (Exception. (str error-page-not-found)))
               (= (.getMessage e) error-unauthorized-friends-ids-access) (throw (Exception. (str error-unauthorized-friends-ids-access)))
               (= (.getMessage e) error-unauthorized-user-timeline-statuses-access) (throw (Exception. (str error-unauthorized-user-timeline-statuses-access)))
               (= endpoint "application/rate-limit-status") (do
                                                              (swap! endpoint-exclusion #(assoc % endpoint (in-15-minutes)))
                                                              (when (string/includes? (.getMessage e) error-rate-limit-exceeded)
                                                                (handle-rate-limit-exceeded-error endpoint token-model token-type-model)
                                                                (try-calling-api call endpoint token-model token-type-model context)))))))))

(defn get-rate-limit-status
  [model token-type-model]
  (let [resources "resources=favorites,statuses,users,lists,friends,friendships,followers"
        twitter-token (twitter-credentials @next-token)
        response (try-calling-api
                   #(application-rate-limit-status :client %
                                                   :oauth-creds twitter-token
                                                   :params {:resources resources})
                   "application/rate-limit-status"
                   model
                   token-type-model
                   "a call to \"application/rate-limit-status\"")
        resources (:resources (:body response))]
    (swap! rate-limits (constantly resources))))

(defn find-next-token
  [token-model token-type-model endpoint context]
  (let [first-available-token (find-first-available-token token-model token-type-model)
        _ (when (nil? first-available-token)
            (set-next-token
              first-available-token
              context))
        next-token-candidate (if @next-token
                               @next-token
                               first-available-token)
        it-is-frozen (is-token-candidate-frozen next-token-candidate)]
    (if it-is-frozen
      (do
        (set-next-token
          (find-first-available-token-when endpoint context token-model token-type-model)
          context)
        (swap! remaining-calls #(assoc % (keyword endpoint) ((keyword endpoint)
                                                             @call-limits))))
      (set-next-token next-token-candidate context))))

(defn how-many-remaining-calls-for
  [endpoint token-model token-type-model]
  (when (nil? (@remaining-calls (keyword endpoint)))
    (do
      (get-rate-limit-status token-model token-type-model)
      (swap! remaining-calls #(assoc % (keyword endpoint) (:limit (get (:users @rate-limits) (keyword "/users/show/:id")))))))
  (get @remaining-calls (keyword endpoint)))

(defn how-many-remaining-calls-for-statuses
  [token-model token-type-model]
  (when (nil? (@remaining-calls (keyword "statuses/show/:id")))
    (do
      (get-rate-limit-status token-model token-type-model)
      (swap! remaining-calls #(assoc
                                %
                                (keyword "statuses/show/:id")
                                (:limit (get (:statuses @rate-limits) (keyword "/statuses/show/:id")))))))
  (get @remaining-calls (keyword "statuses/show/:id")))

(defn how-many-remaining-calls-showing-user
  [token-model token-type-model]
  (how-many-remaining-calls-for "users/show" token-model token-type-model))

(defn update-remaining-calls
  [headers endpoint]
  (let [endpoint-keyword (keyword endpoint)]
    (when
      (and
        (pos? (count headers))
        (not (nil? (:x-rate-limit-remaining headers))))
      (try
        (swap! remaining-calls #(assoc % endpoint-keyword (Long/parseLong (:x-rate-limit-remaining headers))))
        (catch Exception e
          (log/warn (.getMessage e))))
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
        (log/info (str "Rate limit at " limit " for \"" endpoint "\"")))
      (log/info (str remaining-calls " remaining calls for \"" endpoint
                     "\" called with consumer key \"" @current-consumer-key "\"")))

    (update-remaining-calls headers endpoint)))

(defn ten-percent-of
  [dividend]
  (* dividend 0.10))

(defn ten-percent-of-limit
  "Calculate 10 % of the rate limit, return 1 on exception"
  [headers]
  (def percentage (atom 1))
  (try (swap! percentage (constantly (ten-percent-of (Long/parseLong (:x-rate-limit-limit headers)))))
       (catch Exception e (error-handler/log-error e
                                                   (str "An error occurred when calculating "
                                                        "the 10 percent of the rate limit: ")))
       (finally @percentage)))

(defn guard-against-api-rate-limit
  "Wait for 15 min whenever a API rate limit is about to be reached"
  [headers endpoint & [on-reached-api-limit token-model token-type-model]]
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
        (if (some? token-model)
          (handle-rate-limit-exceeded-error "statuses/show/:id" token-model token-type-model)
          (wait-for-15-minutes endpoint)))
      (catch Exception e (error-handler/log-error e)))))

(defn guard-against-exceptional-member
  [member model]
  (if (or
        (= 1 (:is-not-found member))
        (= 1 (:is-protected member))
        (= 1 (:is-suspended member)))
    (new-member member model)
    member))

(defn page-not-found-exception?
  [e]
  (= (.getMessage e) error-page-not-found))

(defn make-unauthorized-statuses-access-response
  [screen-name]
  (do
    (log/info (str "Not authorized to access statuses of " screen-name))
    {:headers {:unauthorized true}
     :body    '()}))

(defn make-not-found-statuses-response
  ([screen-name]
   (do
     (log/info (str "Could not find statuses of " screen-name))
     {:headers {:not-found true}
      :body    '()}))
  ([id status-id]
   (do
     (log/info (str "Could not find status having id #" id " and status-id #" status-id))
     {:headers {:not-found true}
      :body    '()})))

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
  [{screen-name :screen-name id :id} token-model token-type-model member-model]
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
            (handle-rate-limit-exceeded-error "users/show" token-model token-type-model)
            (get-twitter-user-by-id-or-screen-name {screen-name :screen-name id :id} token-model token-type-model member-model))
          (string/includes? (.getMessage e) error-user-not-found)
          (guard-against-exceptional-member {:screen_name         screen-name
                                             :twitter-id          id
                                             :is-not-found        1
                                             :is-protected        0
                                             :is-suspended        0
                                             :total-subscribees   0
                                             :total-subscriptions 0} member-model)
          (string/includes? (.getMessage e) error-user-suspended)
          (guard-against-exceptional-member {:screen_name         screen-name
                                             :twitter-id          id
                                             :is-not-found        0
                                             :is-protected        0
                                             :is-suspended        1
                                             :total-subscribees   0
                                             :total-subscriptions 0} member-model))))))

(defn get-twitter-status-by-id
  [props token-model token-type-model]
  (let [status-id (:status-id props)]
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
          (when (not= (.getMessage e) error-no-status)
            (log/warn (.getMessage e)))
          (cond
            (page-not-found-exception? e) (make-not-found-statuses-response
                                            (:id props)
                                            (:status-id props))
            (string/includes? (.getMessage e) error-rate-limit-exceeded) (do
                                                                           (handle-rate-limit-exceeded-error "statuses/show/:id" token-model token-type-model)
                                                                           (get-twitter-status-by-id props token-model token-type-model))
            (string/includes? (.getMessage e) error-no-status) {:error error-no-status}
            (string/includes? (.getMessage e) error-missing-status-id) {:error error-missing-status-id}
            :else (error-handler e)))))))

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
  [member token-model token-type-model member-model context]
  (if
    (and
      (know-all-about-remaining-calls-and-limit)
      (is-rate-limit-exceeded))
    (do
      (freeze-current-token)
      (find-next-token token-model token-type-model "users/show" context)
      (member-by-prop member token-model token-type-model member-model context))
    (let [twitter-user (get-twitter-user-by-id-or-screen-name member token-model token-type-model member-model)]
      (if (nil? twitter-user)
        (do
          (find-next-token token-model token-type-model "users/show" context)
          (member-by-prop member token-model token-type-model member-model context))
        twitter-user))))

(defn status-by-prop
  [props token-model token-type-model context]
  (if
    (and
      (know-all-about-remaining-calls-and-limit)
      (is-rate-limit-exceeded))
    (do
      (freeze-current-token)
      (find-next-token token-model token-type-model "statuses/show/:id" context)
      (status-by-prop props token-model token-type-model context))
    (let [twitter-status (get-twitter-status-by-id props token-model token-type-model)]
      twitter-status)))

(defn get-member-by-screen-name
  [screen-name token-model token-type-model member-model]
  (let [_ (find-next-token token-model token-type-model "users/show" "trying to call \"users/show\" with a screen name")
        user (member-by-prop {:screen-name screen-name} token-model token-type-model member-model "a call to \"users/show\" with a screen name")
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show")
    (:body user)))

(defn get-member-by-id
  [id token-model token-type-model member-model]
  (let [_ (find-next-token token-model token-type-model "users/show" "trying to call \"users/show\" with an id")
        user (member-by-prop {:id id} token-model token-type-model member-model "a call to \"users/show\" with an id")
        headers (:headers user)]
    (guard-against-api-rate-limit headers "users/show")
    (:body user)))

(defn get-status-by-id
  [{id        :id
    status-id :status-id} token-model token-type-model]
  (let [status (status-by-prop {:status-id status-id :id id} token-model token-type-model "a call to \"statuses/show\" with an id")
        headers (:headers status)]
    (if
      (and
        (some? headers)
        (nil? (:error status)))
      (do
        (guard-against-api-rate-limit headers "statuses/show/:id" nil token-model token-type-model)
        (assoc (:body status) :id id))
      (log/info (str "Could not find status having id #" status-id)))))

(defn get-id-of-member-having-username
  [screen-name member-model token-model token-type-model]
  (let [matching-members (find-member-by-screen-name screen-name member-model)
        member (if matching-members
                 (first matching-members)
                 (get-member-by-screen-name screen-name token-model token-type-model member-model))]
    {:twitter-id (:twitter-id member)
     :id         (:id member)}))

(defn get-subscriptions-by-screen-name
  [screen-name token-model token-type-model]
  (try-calling-api
    #(friends-ids
       :client %
       :oauth-creds (twitter-credentials @next-token)
       :params {:screen-name screen-name})
    "friends/id"
    token-model
    token-type-model
    "a call to \"friends/id\""))

(defn get-subscribers-by-screen-name
  [screen-name tokens-model token-type-model]
  (try-calling-api
    #(followers-ids
       :client %
       :oauth-creds (twitter-credentials @next-token)
       :params {:screen-name screen-name})
    "followers/id"
    tokens-model
    token-type-model
    "a call to \"followers/id\""))

(defn get-subscriptions-of-member
  [screen-name token-model token-type-model on-reached-api-limit]
  (let [_ (find-next-token token-model token-type-model "users/show" "trying to make a call to \"friends/ids\"")
        subscriptions (get-subscriptions-by-screen-name screen-name token-model token-type-model)
        headers (:headers subscriptions)
        friends (:body subscriptions)]
    (guard-against-api-rate-limit headers "friends/ids" on-reached-api-limit)
    (:ids friends)))

(defn get-subscribees-of-member
  [screen-name token-model token-type-model]
  (let [_ (find-next-token token-model token-type-model "users/show" "trying to make a call to \"followers/ids\"")
        subscribees (get-subscribers-by-screen-name screen-name token-model token-type-model)
        headers (:headers subscribees)
        followers (:body subscribees)]
    (guard-against-api-rate-limit headers "followers/ids")
    (:ids followers)))

(defn get-favorites-by-screen-name
  [opts endpoint context tokens-model token-type-model]
  (let [base-params {:count            200
                     :include-entities 1
                     :include-rts      1
                     :exclude-replies  0
                     :screen-name      (:screen-name opts)
                     :trim-user        0
                     :tweet-mode       "extended"}
        params (cond
                 (not (nil? (:since-id opts))) (assoc base-params :since-id (:since-id opts))
                 (not (nil? (:max-id opts))) (assoc base-params :max-id (:max-id opts))
                 :else base-params)]

    (try-calling-api
      #(favorites-list
         :client %
         :oauth-creds (twitter-credentials @next-token)
         :params params)
      endpoint
      tokens-model
      token-type-model
      (str "a " context))))

(defn try-getting-favorites
  [status-getter endpoint opts]
  (let [screen-name (:screen-name opts)
        response (try (status-getter)
                      (catch Exception e
                        (cond
                          (= (.getMessage e) error-rate-limit-exceeded) (do
                                                                          (error-handler/log-error
                                                                            e
                                                                            (str
                                                                              "Could not access favorites of \""
                                                                              screen-name
                                                                              "\" (exceeded rate limit): ")
                                                                            :no-stack-trace)
                                                                          {:headers {:exceeded-rate-limit true}
                                                                           :body    '()})
                          (string/ends-with?
                            (.getMessage e) error-unauthorized-favorites-list-access) (do
                                                                                        (log/info (str "Not authorized to favorites list of " screen-name))
                                                                                        {:headers {:unauthorized true}
                                                                                         :body    '()})
                          :else (error-handler/log-error
                                  e
                                  (str "Could not access favorites of \"" screen-name "\": ")))))
        headers (:headers response)
        statuses (:body response)
        _ (when (and
                  (zero? (count statuses))
                  (:exceeded-rate-limit (:headers response)))
            (wait-for-15-minutes endpoint))
        _ (when (and
                  (zero? (count statuses))
                  (nil? (:exceeded-rate-limit (:headers response)))
                  (nil? (:unauthorized (:headers response))))
            (guard-against-api-rate-limit headers endpoint))]
    statuses))

(defn get-favorites-of-member
  [opts token-model token-type-model]
  (let [endpoint "favorites/list"
        call "call to \"favorites/list\""
        context (str "trying to make a " call)
        _ (find-next-token token-model token-type-model endpoint context)
        favorites-getter #(get-favorites-by-screen-name opts endpoint call token-model token-type-model)
        favorites (try-getting-favorites favorites-getter endpoint opts)]
    favorites))

(defn get-statuses-by-screen-name
  [opts endpoint context tokens-model token-type-model]
  (let [base-params {:count            200
                     :include-entities 1
                     :include-rts      1
                     :exclude-replies  0
                     :screen-name      (:screen-name opts)
                     :trim-user        0
                     :tweet-mode       "extended"}
        params (cond
                 (not (nil? (:since-id opts)))
                 (assoc base-params :since-id (:since-id opts))
                 (not (nil? (:max-id opts)))
                 (assoc base-params :max-id (:max-id opts))
                 :else
                 base-params)]

    (try-calling-api
      #(statuses-user-timeline
         :client %
         :oauth-creds (twitter-credentials @next-token)
         :params params)
      endpoint
      tokens-model
      token-type-model
      (str "a " context))))

(defn exception-message-ends-with?
  [e substring]
  ((.getMessage e)
   string/ends-with? substring))

(defn unauthorized-user-timeline-statuses-access-exception?
  [e]
  (exception-message-ends-with?
    e
    error-unauthorized-user-timeline-statuses-access))

(defn try-getting-statuses
  [status-getter endpoint opts]
  (let [screen-name (:screen-name opts)
        response (try (status-getter)
                      (catch Exception e
                        (cond
                          (unauthorized-user-timeline-statuses-access-exception? e) (make-unauthorized-statuses-access-response screen-name)
                          (page-not-found-exception? e) (make-not-found-statuses-response screen-name)
                          :else (error-handler/log-error
                                  e
                                  (str "Could not access statuses of " screen-name ": ")))))
        headers (:headers response)
        statuses (:body response)
        _ (when (and
                  (nil? (:not-found (:headers response)))
                  (nil? (:unauthorized (:headers response))))
            (guard-against-api-rate-limit headers endpoint))]
    statuses))

(defn get-statuses-of-member
  [opts token-model token-type-model]
  (let [endpoint "statuses/user_timeline"
        call "call to \"statuses/user_timeline\""
        context (str "trying to make a " call)
        _ (find-next-token token-model token-type-model endpoint context)
        status-getter #(get-statuses-by-screen-name opts endpoint call token-model token-type-model)
        statuses (try-getting-statuses status-getter endpoint opts)]
    statuses))

