(ns repository.query-executor
  (:require [korma.core :as db]
            [utils.error-handler :as error-handler])
  (:import
           (java.sql BatchUpdateException)))

(defn guard-against-database-failure
  [f & [max-retries]]
  (let [max-retries (if (some? max-retries)
                      max-retries
                      1)
        maybe-retry (when (> (dec max-retries) 0)
                      (fn []
                        (println (str
                                   "About to retry transaction "
                                   "(" max-retries " more times at most)."))
                        (guard-against-database-failure f (dec max-retries))))]
    (try (f)
         (catch Exception e
           (cond
             (instance? BatchUpdateException e) (maybe-retry)
             :else (error-handler/log-error e))))))

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
    (catch Exception e (error-handler/log-error e)))
  (when (some? finder)
    (finder)))
