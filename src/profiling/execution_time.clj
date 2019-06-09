(ns profiling.execution-time
  (:require [clojure.tools.logging :as log])
  (:use [twitter.date]))

(defn profile
  [fn]
  (let [before (get-timestamp)
        ret (fn)
        after (get-timestamp)
        inMilliseconds (- after before)
        inSeconds (float (/ inMilliseconds 1000))
        inMinutes (float (/ inSeconds 60))]
    (log/info (str
                "Took #" inMilliseconds "ms i.e. "
                inSeconds "sec i.e. "
                inMinutes "min"))
    ret))