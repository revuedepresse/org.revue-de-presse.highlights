(ns command.generate-timely-statuses-test
  (:require [command.generate-timely-statuses :refer :all]
            [clojure.test :refer :all]))

(defn ensure-timely-statuses-are-generated-for-week
  [week]
  (let [timely-statuses (generate-timely-statuses week 2018)]
    (is (= (count timely-statuses) 0))))

(deftest it-should-generate-timely-statuses-
  (let [weeks (take 52 (iterate inc 0))]
    (doall
      (pmap
        #(ensure-timely-statuses-are-generated-for-week %)
        weeks))))