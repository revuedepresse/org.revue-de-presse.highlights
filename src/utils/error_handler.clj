(ns utils.error-handler
  (:require [environ.core :refer [env]]
            [taoensso.timbre :as timbre]))

(defn log-error
  [e & [prefix no-stack-trace]]
  (let [prefix (if (some? prefix)
                 prefix
                 "")
        print-stack-trace (and
                            (nil? no-stack-trace)
                            (true? (:debug env)))]
    (timbre/error (str prefix (.getMessage e)))
    (when print-stack-trace
      (doall
        (map println (.getStackTrace e))))))