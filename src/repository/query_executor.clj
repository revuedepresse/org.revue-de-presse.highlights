(ns repository.query-executor
  (:require [clojure.tools.logging :as log]
            [korma.core :as db])
  (:import (com.mysql.jdbc.exceptions.jdbc4 CommunicationsException)
           (com.mysql.jdbc.exceptions.jdbc4 MySQLTransactionRollbackException)
           (java.sql BatchUpdateException)))

(defn guard-against-database-failure
  [f & [max-retries]]
  (let [max-retries (when (some? max-retries)
                      max-retries
                      1)
        maybe-retry (when (> (dec max-retries) 0)
                      (println (str
                                 "About to retry transaction "
                                 "(" max-retries " more times at most)."))
                      (guard-against-database-failure f (dec max-retries)))]
    (try (f)
         (catch Exception e
           (cond
             (instance? CommunicationsException e) (println (str "Could not communicate with the database."))
             (instance? BatchUpdateException e) (maybe-retry)
             (instance? MySQLTransactionRollbackException e) (maybe-retry)
             :else (log/error (.getException e)))))))

(defn exec-query
  [& args]
  (let [results (guard-against-database-failure #(apply db/exec-raw args))]
    results))

(defn insert-query
  [{values :values
    model  :model}]
  (let [results (guard-against-database-failure #(db/insert model (db/values values)) 2)]
    results))

(defn bulk-insert-and-find-on-condition
  [values model & [finder]]
  (try
    (db/insert model (db/values values))
    (catch Exception e (log/error (.getMessage e))))
  (when (some? finder)
    (finder)))
