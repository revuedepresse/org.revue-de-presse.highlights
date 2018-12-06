(ns command.generate-timely-statuses-test
  (:require [command.generate-timely-statuses :refer :all]
            [clojure.test :refer :all]))

(deftest it-should-generate-timely-statuses
  (loop [week 0]
    (when (< week 52)
      (let [timely-statuses (generate-timely-statuses week 2018)]
        ; It should generate timely statuses from statuses
        (is (= (count timely-statuses) 0)))
      (recur (inc week)))))