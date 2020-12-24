(ns command.generate-keyword-test
  (:require [command.generate-keywords :refer :all]
            [clojure.test :refer :all]))

(defn ensure-keywords-are-generated-for-week
  [week]
  (let [result-map (generate-keywords-for-all-aggregates :date {:week week :year 2018})]
    (is (= (count (:result result-map)) 0))))

(deftest it-should-generate-keywords
  (let [weeks (take 53 (iterate inc 0))]
    (doall
      (pmap
        #(ensure-keywords-are-generated-for-week %)
        weeks))))