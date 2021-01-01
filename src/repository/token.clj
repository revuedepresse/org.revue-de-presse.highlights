(ns repository.token
  (:require [korma.core :as db])
  (:use [korma.db]
        [utils.string]
        [repository.database-schema]
        [repository.status-aggregate]
        [repository.query-executor]))

(declare token)
(declare token-type)

(defn get-token-model
  [connection]
  (db/defentity token
                (db/pk :id)
                (db/table :weaving_access_token)
                (db/database connection)
                (db/entity-fields
                  :token
                  :secret
                  :consumer_key
                  :consumer_secret
                  :frozen_until))
  token)

(defn get-token-type-model
  [connection]
  (db/defentity token-type
                (db/pk :id)
                (db/table :weaving_token_type)
                (db/database connection)
                (db/entity-fields
                  :name))
  token-type)

(defn select-token-types
  [model]
  (->
    (db/select* model)
    (db/fields [:id]
               [:name])))

(defn select-token
  [model]
  (->
    (db/select* model)
    (db/fields [:consumer_key :consumer-key]
               [:consumer_secret :consumer-secret]
               [:frozen_until :frozen-until]
               :token
               :secret)))

(defn freeze-token
  [token]
  (db/exec-raw [(str "UPDATE weaving_access_token "
                     "SET frozen_until = NOW()::timestamp + '15 MINUTES'::INTERVAL "
                     "WHERE token = ?") [token]]))

(defn find-user-token-type
  "Find user token type"
  [model]
  (-> (select-token-types model)
      (db/where (and (= :name "user")))
      (db/select)
      first))

(defn find-first-available-token
  "Find a token which has not been frozen"
  [model token-type-model]
  (let [user-token-type (find-user-token-type token-type-model)]
    (first (-> (select-token model)
               (db/where (and (= :type (:id user-token-type))
                              (not= (db/sqlfn coalesce :consumer_key "__no_consumer_key") "__no_consumer_key")
                              (<= :frozen_until (db/sqlfn now))))
               (db/select)))))

(defn find-first-available-tokens-other-than
  "Find a token which has not been frozen"
  [access-tokens model token-type-model]
  (let [user-token-type (find-user-token-type token-type-model)
        excluded-access-tokens (if access-tokens access-tokens '("_"))
        first-available-token (first (-> (select-token model)
                                         (db/where (and
                                                     (= :type (:id user-token-type))
                                                     (not= (db/sqlfn coalesce :consumer_key "__no_consumer_key") "__no_consumer_key")
                                                     (not-in :token excluded-access-tokens)
                                                     (<= :frozen_until (db/sqlfn now))))
                                         (db/order :frozen_until :ASC)
                                         (db/select)))]
    first-available-token))
