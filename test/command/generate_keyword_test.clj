(ns command.generate-keyword-test
  (:require [command.generate-keywords :refer :all]
            [clojure.test :refer :all]))

(deftest it-should-generate-keywords
  (loop [week 0]
    (when (< week 52)
      (let [timely-statuses (generate-keywords-for-all-aggregates week 2018)]
        (is (= (count timely-statuses) 0)))
      (recur (inc week)))))