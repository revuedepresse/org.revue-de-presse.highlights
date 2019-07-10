(ns utils.error-handler
  (:require [clojure.tools.logging :as log]))

(defn log-error
  [e & [prefix no-stack-trace]]
  (let [prefix (if (some? prefix)
                 prefix
                 "")]
    (log/error (str prefix (.getMessage e)))
    (when-not no-stack-trace
      (doall
        (map println (.getStackTrace e))))))