(ns utils.error-handler
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]))

(defn log-error
  [e & [prefix no-stack-trace]]
  (let [prefix (if (some? prefix)
                 prefix
                 "")
        print-stack-trace (and
                            (nil? no-stack-trace)
                            (true? (:debug env)))]
    (log/error (str prefix (.getMessage e)))
    (when print-stack-trace
      (doall
        (map println (.getStackTrace e))))))