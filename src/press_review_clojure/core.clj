(ns press_review_clojure.core
  (:require [clj-http.client :as client])
  (:require [cheshire.core :refer :all])
  (:gen-class main true))

(defn get_statuses
  ([] (def apiToken "ec5610735b9d646e569e57d02b1c9411794b53de")
    (def route "http://localhost:8090/api/twitter/tweet/latest/programming__clojure")
    (def statuses (client/get route
              {:headers {:x-auth-token apiToken}}))
    (def body (statuses :body))
    (parse-string body)
   )
  )

(defn -main
  [& args]
  (println get_statuses))
