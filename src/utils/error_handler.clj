(ns utils.error-handler
  (:require [clojure.tools.logging :as log]))

(defn log-error
  [e & [prefix]]
  (let [prefix (if (some? prefix)
                 prefix
                 "")]
    (log/error (str prefix (.getMessage e)))
    (doall
      (map println (.getStackTrace e)))))