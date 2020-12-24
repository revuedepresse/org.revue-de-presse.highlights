(ns repository.status-identity
  (:require [korma.core :as db]
            [clojure.string :as string])
  (:use [korma.db]
        [repository.database-schema]
        [repository.query-executor]
        [utils.string]))

(declare status-identity)

(defn get-status-identity-model
  [connection]
  (db/defentity status-identity
                (db/table :status_identity)
                (db/database connection)
                (db/entity-fields
                  :id
                  :member_identity
                  :twitter_id
                  :publication_date_time
                  :status
                  :archived_status))
  status-identity)

(defn select-fields
  [model member-identity-model]
  (let [member-identity-id-col (get-column "id" member-identity-model)
        member-id-col (get-column "member_id" member-identity-model)
        member-name-col (get-column "screen_name" member-identity-model)
        member-twitter-id-col (get-column "twitter_id" member-identity-model)]
    (->
      (db/select* model)
      (db/fields :id
                 [member-identity-id-col :member-identity-id]
                 [member-name-col :member-name]
                 [member-id-col :member-id]
                 [member-twitter-id-col :member-twitter-id]
                 [:twitter_id :status-twitter-id]
                 [:publication_date_time :publication-date-time]
                 [:status :status-id]
                 [:archived_status :archived-status-id])
      (db/join member-identity-model (= member-identity-id-col :member_identity)))))

(defn find-by-aggregate-id
  [aggregate-id week year db]
  (let [query (str "
                SELECT
                UUID() as \"id\",
                m.id AS \"member-identity\",
                s.ust_api_document AS \"status-api-document\",
                s.ust_created_at AS \"publication-date-time\",
                s.ust_id AS \"status-id\"
                FROM weaving_status_aggregate sa
                INNER JOIN weaving_aggregate a
                ON sa.aggregate_id = a.id
                INNER JOIN weaving_status s
                ON (
                  sa.status_id = s.ust_id
                  AND EXTRACT(WEEK FROM s.ust_created_at) = ?
                  AND EXTRACT(YEAR FROM s.ust_created_at) = ?
                  AND a.screen_name = s.ust_full_name
                  AND s.ust_status_id " (get-collation) " NOT IN (
                    SELECT twitter_id FROM status_identity
                  )
                )
                INNER JOIN member_identity m
                ON (
                  s.ust_full_name " (get-collation) " = m.screen_name
                )
                INNER JOIN weaving_user u
                ON u.usr_id = m.member_id
                WHERE sa.aggregate_id = ?
                GROUP BY s.ust_status_id
              ")]
    (binding [*current-db* db]
      (db/exec-raw [query [week year aggregate-id]] :results))))

(defn count-for-aggregate-id
  [aggregate-id week year db]
  (let [query (str "
                SELECT
                COUNT(*) AS \"total-statuses\"
                FROM weaving_status_aggregate sa
                INNER JOIN weaving_aggregate a
                ON sa.aggregate_id = a.id
                INNER JOIN weaving_status s
                ON (
                    EXTRACT (WEEK FROM s.ust_created_at) = ?
                    AND EXTRACT(YEAR FROM s.ust_created_at) = ?
                    AND sa.status_id = s.ust_id
                    AND a.screen_name = s.ust_full_name
                    AND s.ust_status_id " (get-collation) " NOT IN (
                      SELECT twitter_id FROM status_identity
                    )
                )
                WHERE
                sa.aggregate_id = ?
              ")]
    (binding [*current-db* db]
      (:total-statuses (first (db/exec-raw [query [week year aggregate-id]] :results))))))

(defn find-status-identity-by-twitter-ids
  [twitter-ids {status-identity-model :status-identity
                member-identity-model :member-identity}]
  (let [ids (if (pos? (count twitter-ids)) twitter-ids '(0))
        matching-records (-> (select-fields status-identity-model member-identity-model)
                             (db/where {:twitter_id [in ids]})
                             (db/select))]
    (if matching-records
      matching-records
      '())))

(defn find-status-identity-by-status-ids
  [status-ids {status-identity-model :status-identity
                member-identity-model :member-identity}]
  (let [ids (if (pos? (count status-ids)) status-ids '(0))
        matching-records (-> (select-fields status-identity-model member-identity-model)
                             (db/where {:status [in ids]})
                             (db/select))]
    (if matching-records
      matching-records
      '())))

(defn get-min-week-year-for-aggregate-id
  [aggregate-id db]
  (let [query (str "
                SELECT
                EXTRACT(YEAR FROM s.ust_created_at) as \"since-year\",
                EXTRACT(WEEK FROM s.ust_created_at) as \"since-week\",
                s.ust_id as \"status-id\"
                FROM weaving_status_aggregate sa
                INNER JOIN weaving_aggregate a
                ON sa.aggregate_id = a.id
                INNER JOIN weaving_status s
                ON sa.status_id = s.ust_id
                AND a.screen_name = s.ust_full_name " (get-collation) "
                WHERE sa.aggregate_id = ?
                AND s.ust_created_at IN (
                    SELECT MIN(s.ust_created_at)
                    FROM weaving_status_aggregate sa
                    INNER JOIN weaving_aggregate a
                    ON sa.aggregate_id = a.id
                    INNER JOIN weaving_status s
                    ON (
                      sa.status_id = s.ust_id
                      AND s.ust_status_id " (get-collation) " NOT IN (
                        SELECT twitter_id FROM status_identity
                      )
                    )
                    AND a.screen_name = s.ust_full_name " (get-collation) "
                    WHERE sa.aggregate_id = ?
                )
              ")]
    (binding [*current-db* db]
      (first (db/exec-raw [query [aggregate-id aggregate-id]] :results)))))

(defn bulk-insert-status-identities
  [props
   {aggregate-id :aggregate-id
    week         :week
    year         :year}
   db]
  (let [total-status-identities (count props)
        params (flatten
                 (map
                   #(apply list [(:id %)
                                 (:member-identity %)
                                 (:twitter-id %)
                                 (:publication-date-time %)
                                 (:status-id %)
                                 nil])
                   props))
        bindings (take total-status-identities (iterate
                                                 (constantly "( ? , ? , ? , ? , ? , ? )")
                                                 "( ? , ? , ? , ? , ? , ? )"))
        query (str "
                INSERT INTO status_identity
                (
                  id,
                  member_identity,
                  twitter_id,
                  publication_date_time,
                  status,
                  archived_status
                ) VALUES
                " (string/join "," bindings) "
              ")]
    (println (str
               total-status-identities
               " member identities are about to be added for aggregate #"
               aggregate-id ", week " week " and year " year))
    (db/exec-raw db [query params])))