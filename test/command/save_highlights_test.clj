(ns command.save-highlights-test
  (:require [command.save-highlights :refer :all]
            [clojure.test :refer :all]))

(deftest it-should-save-highlights
  (let [new-highlights (save-highlights "2018-01-01")]
    ; It should save highlights from timely statuses
    (is (= (count new-highlights) 0)))
  (let [new-highlights (save-highlights "01" "2018")]
    ; It should save highlights of January from timely statuses
    (is (= (count new-highlights) 0)))
  (let [today-highlights (save-today-highlights)]
    ; It should today highlights from timely statuses
    (is (= (count today-highlights) 0))))
