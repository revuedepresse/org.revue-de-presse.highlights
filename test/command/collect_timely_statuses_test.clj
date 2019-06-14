(ns command.collect-timely-statuses-test
  (:require [command.collect-timely-statuses :refer :all]
            [clojure.test :refer :all]))

(defn ensure-timely-statuses-are-collected-for-week
  [week]
  (let [timely-statuses (collect-timely-statuses week 2018)]
    (is (= (count timely-statuses) 0))))

(deftest it-should-collect-timely-statuses
  (let [weeks (take 52 (iterate inc 0))]
    (doall
      (pmap
        #(ensure-timely-statuses-are-collected-for-week %)
        weeks))))