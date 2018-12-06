(ns command.unarchive-statuses-test
  (:require [command.unarchive-statuses :refer :all]
            [clojure.test :refer :all]))

(deftest it-should-unarchive-statuses
  (loop [week 0]
    (when (< week 52)
      (let [timely-statuses (unarchive-statuses week 2018)]
        ; It should migrate statuses from the archive table to the live table
        (is (= (count timely-statuses) 0)))
      (recur (inc week)))))