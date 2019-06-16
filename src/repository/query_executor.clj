(ns repository.query-executor
  (:require [clojure.tools.logging :as log]
            [korma.core :as db])
  (:import (com.mysql.jdbc.exceptions.jdbc4 CommunicationsException)))

(defn guard-against-communication-failure
  [f]
  (try (f)
       (catch Exception e
         (cond
           (instance? CommunicationsException e)
           (println (str "Could not communicate with the database."))
           :else
           (log/error (.getException e))))))

(defn exec-query
  [& args]
  (let [results (guard-against-communication-failure #(apply db/exec-raw args))]
    results))

(defn bulk-insert-and-find-on-condition
  [values model & [finder]]
  (try
    (db/insert model (db/values values))
    (catch Exception e (log/error (.getMessage e))))
  (when (some? finder)
    (finder)))
