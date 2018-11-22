(ns command.generate-timely-statuses-test
  (:require [command.generate-timely-statuses :refer :all]
            [clojure.test :refer :all]))

(deftest it-should-generate-timely-statuses
  (let [timely-statuses (generate-timely-statuses)]
    ; It should generate to timely statuses
    (is (= 0 (count timely-statuses)))))