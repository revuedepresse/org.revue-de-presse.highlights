(ns repository.query-executor
  (:require [clojure.tools.logging :as log]
            [korma.core :as db])
  (:import (com.mysql.jdbc.exceptions.jdbc4 CommunicationsException)))

(defn exec-query
  [& args]
  (let [results (try (apply db/exec-raw args)
                     (catch Exception e
                       (cond
                         (instance? CommunicationsException e)
                         (println (str "Could not communicate with the database."))
                         :else
                         (log/error (.getException e)))))]
    results))
