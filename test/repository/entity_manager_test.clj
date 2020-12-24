(ns repository.entity-manager-test
  (:require [environ.core :refer [env]]
            [clojure.test :refer :all])
  (:use [repository.entity-manager]))

(deftest it-should-provide-with-an-entity-manager
  (let [models (get-entity-manager (:database env))]
    ; Does the entity manager manager return
    ; a map of all the models?
    (is (map? models))
    ; There are 23 models so far
    ; some being duplicates of the same table
    ; representing the same data in distinct contexts
    (is (= 24 (count (keys models))))))
