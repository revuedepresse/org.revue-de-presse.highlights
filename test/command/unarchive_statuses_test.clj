(ns command.unarchive-statuses-test
  (:require [command.unarchive-statuses :refer :all]
            [clojure.test :refer :all]))

(deftest it-should-unarchive-statuses
  (loop [month 0]
    (when (< month 51)
      (let [timely-statuses (unarchive-statuses month 2018)]
        ; It should migrate statuses from the archive table to the live table
        (is (= (count timely-statuses) 0)))
      (recur (inc month)))))