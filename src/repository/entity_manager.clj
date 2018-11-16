(ns repository.entity-manager
    (:require [korma.core :as db]
              [clojure.string :as string]
              [clojure.edn :as edn]
              [clojure.tools.logging :as log]
              [pandect.algo.sha1 :refer :all]
              [clj-uuid :as uuid])
    (:use [korma.db]))

(defn connect-to-db
  "Create a connection and provide with a map of entities"
  ; @see https://mathiasbynens.be/notes/mysql-utf8mb4
  ; @see https://clojurians-log.clojureverse.org/sql/2017-04-05
  [config]
  (defdb database-connection {:classname "com.mysql.jdbc.Driver"
                              :subprotocol "mysql"
                              :subname (str "//" (:host config) ":" (:port config) "/" (:name config))
                              :useUnicode "yes"
                              :characterEncoding "UTF-8"
                              :characterSet "utf8mb4"
                              :collation "utf8mb4_unicode_ci"
                              :delimiters "`"
                              :user (:user config)
                              :password (:password config)})

  (declare tokens
           aggregate
           users members
           subscriptions subscribees
           member-subscriptions member-subscribees
           liked-status status)

  (db/defentity aggregate
                (db/table :weaving_aggregate)
                (db/database database-connection)
                (db/entity-fields
                  :id
                  :name
                  :created_at
                  :locked
                  :locked_at
                  :unlocked_at
                  :screen_name
                  :list_id))

  (db/defentity status
                (db/pk :ust_id)
                (db/table :weaving_status)
                (db/database database-connection)
                (db/entity-fields
                  :ust_id
                  :ust_hash                                 ; sha1(str ust_text  ust_status_id)
                  :ust_text
                  :ust_full_name                            ; twitter user screen name
                  :ust_name                                 ; twitter user full name
                  :ust_access_token
                  :ust_api_document
                  :ust_created_at
                  :ust_status_id))

  (db/defentity archived-status
                (db/table :weaving_archived_status)
                (db/database database-connection)
                (db/entity-fields
                  :ust_id
                  :ust_hash                                 ; sha1(str ust_text  ust_status_id)
                  :ust_text
                  :ust_full_name                            ; twitter user screen name
                  :ust_name                                 ; twitter user full name
                  :ust_access_token
                  :ust_api_document
                  :ust_created_at
                  :ust_status_id))

  (db/defentity liked-status
                (db/table :liked_status)
                (db/database database-connection)
                (db/entity-fields
                  :id
                  :status_id
                  :archived_status_id
                  :time_range
                  :is_archived_status
                  :member_id
                  :member_name
                  :liked_by
                  :liked_by_member_name
                  :aggregate_id
                  :aggregate_name))

  (db/defentity tokens
                (db/table :weaving_access_token)
                (db/database database-connection)
                (db/entity-fields
                  :token
                  :secret
                  :consumer_key
                  :consumer_secret
                  :frozen_until))

  ; It seems that duplicates are required to express the relationships
  ; @see https://github.com/korma/Korma/issues/281
  (db/defentity users
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :usr_email
                  :not_found
                  :protected
                  :suspended))

  (db/defentity members
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended
                  :usr_locked
                  :usr_status
                  :description
                  :total_subscribees
                  :total_subscriptions))

  (db/defentity subscriptions
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended))

  (db/defentity subscribees
                (db/pk :usr_id)
                (db/table :weaving_user)
                (db/database database-connection)
                (db/entity-fields
                  :usr_id
                  :usr_twitter_username
                  :usr_twitter_id
                  :not_found
                  :protected
                  :suspended))

  (db/defentity member-subscriptions
                (db/pk :id)
                (db/table :member_subscription)
                (db/database database-connection)
                (db/entity-fields :member_id :subscription_id)
                (db/has-one members {:fk :usr_id})
                (db/has-one subscriptions {:fk :usr_id}))

  (db/defentity member-subscribees
                (db/pk :id)
                (db/table :member_subscribee)
                (db/database database-connection)
                (db/entity-fields :member_id :subscribee_id)
                (db/has-one members {:fk :usr_id})
                (db/has-one subscribees {:fk :usr_id}))

  {:aggregates aggregate
   :archived-status archived-status
   :users users
   :liked-status liked-status
   :members members
   :subscribees subscribees
   :subscriptions subscriptions
   :member-subscriptions member-subscriptions
   :member-subscribees member-subscribees
   :status status
   :tokens tokens})

(defn get-entity-manager
  [config]
  (connect-to-db (edn/read-string config)))

(defn find-aggregate-by-id
  "Find an aggregate by id"
  [id model]
  (->
    (db/select* model)
    (db/fields :id
               :name
               [:screen_name :screen-name])
    (db/where {:id id})
    (db/select)))

(defn select-statuses
  [model]
  (->
    (db/select* model)
    (db/fields [:ust_id :id]
               [:ust_hash :hash]
               [:ust_text :text]
               [:ust_full_name :screen-name]
               [:ust_name :name]
               [:ust_access_token :access-token]
               [:ust_api_document :document]
               [:ust_created_at :created-at]
               [:ust_status_id :twitter-id])))

(defn find-status-by-twitter-id
  "Find a status by id"
  [twitter-id model]
  (-> (select-statuses model)
      (db/where {:ust_status_id twitter-id})
      (db/select)))

(defn find-statuses-having-ids
  "Find statuses by theirs Twitter ids"
  [ids model]
  (let [ids (if ids ids '(0))
        matching-statuses (-> (select-statuses model)
                             (db/where {:ust_status_id [in ids]})
                             (db/select))]
    (if matching-statuses
      matching-statuses
      '())))

(defn find-statuses-having-ids-of-authors
  "Find statuses by theirs Twitter ids and authors name"
  [statuses-ids screen-names]
  (let [ids (if statuses-ids statuses-ids '(0))
        names (if screen-names screen-names '(""))
        params (interpose "," (take (count ids) (iterate (constantly "(?,?)") "(?,?)")))
        question-marks (clojure.string/join "" (map str params))
        params-values (interleave ids names)
        results (db/exec-raw [(str
                                "SELECT ust_status_id as `twitter-id`, ust_id as `id`, ust_full_name as `screen-name`"
                                "FROM weaving_status "
                                "WHERE (ust_status_id, ust_full_name) in (" question-marks ")") params-values] :results)]
    (log/info (str "There are " (count results) " matching results."))
    results))

(defn new-status
  [status model]
  (let [{text :text
         screen-name :screen-name
         name :name
         access-token :token
         avatar :avatar
         document :document
         created-at :created-at
         twitter-id :twitter-id} status]

    (log/info (str "About to insert status #" twitter-id
                   " authored by \"" screen-name "\""))

    (try
      (db/insert model
        (db/values [{:ust_hash (sha1 (str text twitter-id))
                    :ust_text text
                    :ust_full_name screen-name
                    :ust_name name
                    :ust_avatar avatar
                    :ust_access_token access-token
                    :ust_api_document document
                    :ust_created_at created-at
                    :ust_status_id twitter-id}]))
      (catch Exception e (log/error (.getMessage e))))

    (first (find-status-by-twitter-id twitter-id model))))

(defn replace-underscore-with-dash
  [[k v]]
  (let [key-value [(keyword (string/replace (name k) #"-" "_") ) v]]
    key-value))

(defn snake-case-keys
  [m]
  (let [props-having-converted-keys (->> (map replace-underscore-with-dash m)
                                         (into {}))]
    props-having-converted-keys))

(defn get-prefixer
  [prefix]
  (fn [[k v]]
    (let [keyword-name (name k)
          prefixed-keyword-name (str prefix keyword-name)
          key-value [(keyword prefixed-keyword-name) v]]
      key-value)))

(defn prefixed-keys
  [m]
  (let [prefixer (get-prefixer "ust_")
        props-having-converted-keys (->> (map prefixer m)
                                         (into {}))]
    props-having-converted-keys))

(defn get-status-hash
  [status]
  (let [twitter-id (:status_id status)
        concatenated-string (str (:text status) twitter-id)
        hash (sha1 concatenated-string)]
    (log/info (str "Hash for " twitter-id " is \"" hash "\""))
    hash))


      (defn assoc-hash
  [status]
  (assoc status :hash (get-status-hash status)))

(defn bulk-insert-new-statuses
  [statuses model]
  (let [snake-cased-values (map snake-case-keys statuses)
        statuses-values (map assoc-hash snake-cased-values)
        deduped-statuses (dedupe (sort-by #(:status_id %) statuses-values))
        prefixed-keys-values (map prefixed-keys deduped-statuses)
        twitter-ids (map #(:ust_status_id %) prefixed-keys-values)]
    (if (pos? (count twitter-ids))
      (do
        (try
          (db/insert model (db/values prefixed-keys-values))
          (catch Exception e (log/error (.getMessage e))))
        (find-statuses-having-ids twitter-ids model))
      '())))

(defn get-column
  [column-name model]
  (keyword (str (:table model) "." column-name)))

(defn find-liked-status-by-id
  "Find a liked status by id"
  [id model status-model]
  (let [status-id-col (get-column "ust_id" status-model)
        twitter-id-col (get-column "ust_status_id" status-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [twitter-id-col :twitter-id]
                 [:status_id :status-id]
                 [:archived_status_id :archived-status-id]
                 [:time_range :time-range]
                 [:is_archived_status :is-archived-status]
                 [:member_id :member-id]
                 [:member_name :member-name]
                 [:liked_by :liked-by]
                 [:liked_by_member_name :liked-by-member-name]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name])
      (db/join status-model (= status-id-col :status_id))
      (db/where {:id id})
      (db/select))))

(defn find-liked-statuses-by-triples
  [triples]
  (let [params (interpose "," (take (/ (count triples) 3) (iterate (constantly "(?,?,?)") "(?,?,?)")))
        question-marks (clojure.string/join "" (map str params))
        results (db/exec-raw [(str
                                "SELECT status_id as `status-id`, member_id as `member-id`, liked_by as `liked-by` "
                                "FROM liked_status "
                                "WHERE (status_id, member_id, liked_by) in (" question-marks ")") triples] :results)]
    (log/info (str "There are " (count results) " matching results."))
    results))

(defn find-liked-statuses
  [liked-statuses-ids model status-model]
  (let [status-id-col (get-column "ust_id" status-model)
        twitter-id-col (get-column "ust_status_id" status-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [twitter-id-col :twitter-id]
                 [:status_id :status-id]
                 [:archived_status_id :archived-status-id]
                 [:time_range :time-range]
                 [:is_archived_status :is-archived-status]
                 [:member_id :member-id]
                 [:member_name :member-name]
                 [:liked_by :liked-by]
                 [:liked_by_member_name :liked-by-member-name]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name])
      (db/join status-model (= status-id-col :status_id))
      (db/where {:id [in liked-statuses-ids]})
      (db/select))))

(defn find-liked-status-by
  "Find a liked status by ids of status, members"
  [member-id liked-by-member-id status-id model status-model]
  (let [status-id-col (get-column "ust_id" status-model)
        twitter-id-col (get-column "ust_status_id" status-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [twitter-id-col :twitter-id]
                 [:status_id :status-id]
                 [:archived_status_id :archived-status-id]
                 [:time_range :time-range]
                 [:is_archived_status :is-archived-status]
                 [:member_id :member-id]
                 [:member_name :member-name]
                 [:liked_by :liked-by]
                 [:liked_by_member_name :liked-by-member-name]
                 [:aggregate_id :aggregate-id]
                 [:aggregate_name :aggregate-name])
      (db/join status-model (= status-id-col :status_id))
      (db/where {:liked_by liked-by-member-id
                 :member_id member-id
                 :status_id status-id})
      (db/select))))

(defn new-liked-status
  [liked-status model status-model]
  (let [{status-id :status-id
         archived-status-id :archived-status-id
         publication-date-time :publication-date-time
         time-range :time-range
         is-archived-status :is-archived-status
         member-id :member-id
         member-name :member-name
         liked-by :liked-by
         liked-by-member-name :liked-by-member-name
         aggregate-id :aggregate-id
         aggregate-name :aggregate-name} liked-status
        id (uuid/to-string (uuid/v1))]
    (log/info (str "About to insert status liked by \"" liked-by-member-name
                   "\" authored by \"" member-name "\""))
    (try
      (db/insert model
                 (db/values [{:id id
                              :status_id status-id
                              :archived_status_id archived-status-id
                              :time_range time-range
                              :is_archived_status is-archived-status
                              :member_id member-id
                              :member_name member-name
                              :publication_date_time publication-date-time
                              :liked_by liked-by
                              :liked_by_member_name liked-by-member-name
                              :aggregate_id aggregate-id
                              :aggregate_name aggregate-name}]))
      (catch Exception e (log/error (.getMessage e))))
    (first (find-liked-status-by-id id model status-model))))

(defn new-liked-statuses
  [liked-statuses model status-model]
  (let [identified-liked-statuses (map #(assoc
                                          %
                                          :id
                                          (uuid/to-string (uuid/v1)))
                                       liked-statuses)
        liked-status-values (map snake-case-keys identified-liked-statuses)
        ids (map #(:id %) identified-liked-statuses)]
    (if (pos? ( count ids))
      (do
        (try
          (db/insert model
                     (db/values liked-status-values))
          (catch Exception e (log/error (.getMessage e))))
        (find-liked-statuses ids model status-model))
      '())))

(defn update-min-favorite-id-for-member-having-id
  [min-favorite-id member-id model]
  (db/update model
    (db/set-fields {:min_like_id min-favorite-id})
    (db/where {:usr_id member-id})))

(defn update-max-favorite-id-for-member-having-id
  [max-favorite-id member-id model]
  (db/update model
    (db/set-fields {:max_like_id max-favorite-id})
    (db/where {:usr_id member-id})))

(defn find-member-by-twitter-id
  "Find a member by her / his twitter id"
  [id members]
  (->
    (db/select* members)
    (db/fields [:usr_id :id]
               [:usr_twitter_id :twitter-id]
               [:usr_twitter_username :screen-name])
    (db/where {:usr_twitter_id id})
    (db/select)))

(defn find-member-by-screen-name
  "Find a member by her / his username"
  [screen-name members]
  (->
    (db/select* members)
    (db/fields [:usr_id :id]
               [:usr_twitter_id :twitter-id]
               [:usr_twitter_username :screen-name]
               [:min_like_id :min-favorite-status-id]
               [:max_like_id :max-favorite-status-id])
    (db/where {:usr_twitter_username screen-name})
    (db/select)))

(defn select-members
  [members]
  (-> (db/select* members)
      (db/fields [:usr_id :id]
                 [:usr_twitter_id :twitter-id]
                 [:usr_twitter_username :screen_name]
                 [:usr_twitter_username :screen-name])))

(defn find-member-by-id
  "Find a member by her / his Twitter id"
  [twitter-id members]
  (let [matching-members (-> (select-members members)
                             (db/where {:usr_twitter_id twitter-id})
                             (db/select))]
    (first matching-members)))

(defn find-members-having-ids
  "Find members by theirs Twitter ids"
  [twitter-ids members]
  (let [ids (if twitter-ids twitter-ids '(0))
        matching-members (-> (select-members members)
                             (db/where {:usr_twitter_id [in ids]})
                             (db/select))]
    (if matching-members
      matching-members
      '())))

(defn select-tokens
  [model]
  (->
    (db/select* model)
    (db/fields [:consumer_key :consumer-key]
               [:consumer_secret :consumer-secret]
               :token
               :secret)))

(defn freeze-token
  [consumer-key model]
  (db/exec-raw [(str "UPDATE weaving_access_token "
                     "SET frozen_until = DATE_ADD(NOW(), INTERVAL 15 MINUTE) "
                     "WHERE consumer_key = ?") [consumer-key]]))

(defn find-first-available-tokens
  "Find a token which has not been frozen"
  [model]
  (first (-> (select-tokens model)
    (db/where (and (= :type 1)
                    (not= (db/sqlfn coalesce :consumer_key -1) -1)
                    (<= :frozen_until (db/sqlfn now))))
    (db/select))))

(defn find-first-available-tokens-other-than
  "Find a token which has not been frozen"
  [consumer-keys model]
  (let [excluded-consumer-keys (if consumer-keys consumer-keys '("_"))
        first-available-token (first (-> (select-tokens model)
                                (db/where (and
                                            (= :type 1)
                                            (not= (db/sqlfn coalesce :consumer_key -1) -1)
                                            (not-in :consumer_key excluded-consumer-keys)
                                            (<= :frozen_until (db/sqlfn now))))
                                (db/select)))]
    first-available-token))

(defn select-member-subscriptions
  [model]
  (-> (db/select* model)
      (db/fields :id
                 :member_id
                 :subscription_id)))

(defn find-member-subscriptions-by
  "Find multiple member subscriptions"
  ; @see https://clojure.org/guides/destructuring#_where_to_destructure
  [{:keys [member-id subscriptions-ids]} model]
  (let [ids (if subscriptions-ids subscriptions-ids '(0))]
    (-> (select-member-subscriptions model)
                                   (db/where (and (= :member_id member-id)
                                                  (in :subscription_id ids)))
                                   (db/select))))

(defn select-member-subscribees
  [model]
  (-> (db/select* model)
      (db/fields :id
                 :member_id
                 :subscribee_id)))

(defn find-member-subscribees-by
  "Find multiple member subscribees"
  [{:keys [member-id subscribees-ids]} model]
  (let [ids (if subscribees-ids subscribees-ids '(0))]
    (-> (select-member-subscribees model)
                                   (db/where (and (= :member_id member-id)
                                                  (in :subscribee_id ids)))
                                   (db/select))))

(defn map-get-in
  "Return a map of values matching the provided key coerced to integers"
  ; @see https://stackoverflow.com/a/31846875/282073
  [k coll]
  (let [coerce #(if (number? %) % (Long/parseLong %))
        get-val #(get-in % [k])
        get-coerced #(try (-> % get-val coerce)
         (catch Exception e (log/error (.getMessage e))))]
     (map get-coerced coll)))

(defn deduce-ids-of-missing-members
  [matching-members ids]
  (let [matching-ids (map-get-in :twitter-id matching-members)]
    (clojure.set/difference (set ids) (set matching-ids))))

(defn new-member-subscriptions
  [member-subscriptions model]
  (db/insert model (db/values member-subscriptions)))

(defn new-member-subscribees
  [member-subscribees model]
  (db/insert model (db/values member-subscribees)))

(defn screen-name-otherwise-twitter-id
  [{is-protected :is-protected
    is-suspended :is-suspended
    is-not-found :is-not-found
    screen-name :screen-name
    twitter-id :twitter-id}]
  (if (and
        (or is-not-found is-protected is-suspended)
        (not screen-name))
      twitter-id
      screen-name))

(defn new-member
  [member members]
  (let [{twitter-id :twitter-id
         description :description
         total-subscribees :total-subscribees
         total-subscriptions :total-subscriptions
         is-protected :is-protected
         is-suspended :is-suspended
         is-not-found :is-not-found} member
         member-screen-name (screen-name-otherwise-twitter-id member)]

    (log/info (str "About to insert member with twitter id #" twitter-id
              " and twitter screen mame \"" member-screen-name "\""))

    (try
      (db/insert members
               (db/values [{:usr_position_in_hierarchy 1    ; to discriminate test user from actual users
                            :usr_twitter_id twitter-id
                            :usr_twitter_username member-screen-name
                            :usr_locked false
                            :usr_status false
                            :usr_email (str "@" member-screen-name)
                            :description description
                            :not_found is-not-found
                            :suspended is-suspended
                            :protected is-protected
                            :total_subscribees total-subscribees
                            :total_subscriptions total-subscriptions}]))
      (catch Exception e (log/error (.getMessage e))))
    (find-member-by-id twitter-id members)))

(defn bulk-insert-new-members
  [members model]
  (let [snake-cased-values (map snake-case-keys members)
        members-values (map
                         #(dissoc
                            (assoc
                              %
                              :usr_position_in_hierarchy 1
                              :usr_locked false
                              :usr_status false
                              :usr_email (str "@" (:screen_name %))
                              :usr_twitter_username (:screen_name %)
                              :usr_twitter_id (:twitter_id %)
                              :not_found (:is_not_found %)
                              :protected (:is_protected %)
                              :suspended (:is_suspended %))
                            :screen_name
                            :twitter_id
                            :is_not_found
                            :is_protected
                            :is_suspended)
                         snake-cased-values)
        deduped-values (dedupe (sort-by #(:usr_twitter_id %) members-values))
        twitter-ids (map #(:usr_twitter_id %) deduped-values)]
    (if (pos? (count members-values))
      (do
        (try
          (db/insert model (db/values deduped-values))
          (catch Exception e (log/error (.getMessage e))))
        (find-members-having-ids twitter-ids model))
      '())))

(defn create-member-subscription-values
  [member-id]
  (fn [subscription-id]
    {:id              (uuid/to-string (uuid/v1))
     :member_id       member-id
     :subscription_id subscription-id}))

(defn create-member-subscribee-values
  [member-id]
  (fn [subscribee-id]
    {:id              (uuid/to-string (uuid/v1))
     :member_id       member-id
     :subscribee_id subscribee-id}))

(defn ensure-subscriptions-exist-for-member-having-id
  [{member-id :member-id
    model     :model
    matching-subscriptions-members-ids :matching-subscriptions-members-ids}]
  (let [existing-member-subscriptions (find-member-subscriptions-by {:member-id         member-id
                                                                     :subscriptions-ids matching-subscriptions-members-ids}
                                                                    model)
        existing-member-subscriptions-ids (map #(:subscription_id %) existing-member-subscriptions)
        member-subscriptions (clojure.set/difference (set matching-subscriptions-members-ids) (set existing-member-subscriptions-ids))
        missing-member-subscriptions (map (create-member-subscription-values member-id) member-subscriptions)]

    (log/info (str "About to ensure " (count missing-member-subscriptions)
                   " subscriptions for member having id #" member-id " are recorded."))

    (new-member-subscriptions missing-member-subscriptions model)

    (log/info (str (count missing-member-subscriptions)
                 " subscriptions have been recorded successfully"))))

(defn ensure-subscribees-exist-for-member-having-id
  [{member-id :member-id
    model     :model
    matching-subscribees-members-ids :matching-subscribees-members-ids}]
  (let [existing-member-subscribees (find-member-subscribees-by {:member-id member-id
                                                                 :subscribees-ids matching-subscribees-members-ids}
                                                                model)
        existing-member-subscribees-ids (map #(:subscribee_id %) existing-member-subscribees)
        member-subscribees (clojure.set/difference (set matching-subscribees-members-ids) (set existing-member-subscribees-ids))
        missing-member-subscribees (map (create-member-subscribee-values member-id) member-subscribees)]

    (log/info (str "About to ensure " (count missing-member-subscribees)
                   " subscribees for member having id #" member-id " are recorded."))

    (new-member-subscribees missing-member-subscribees model)

    (log/info (str (count missing-member-subscribees)
                 " subscribees have been recorded successfully"))))


