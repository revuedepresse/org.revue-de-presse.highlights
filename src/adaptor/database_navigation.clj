(ns adaptor.database-navigation
  (:require [environ.core :refer [env]])
  (:use [formatting.formatter]
        [repository.publishers-list]
        [repository.database-schema]
        [repository.entity-manager]
        [repository.keyword]
        [repository.highlight]
        [repository.member]
        [repository.member-subscription]
        [repository.status]
        [repository.status-aggregate]
        [utils.string]))

(defn adapt-results
  [{props :props finder :finder formatter :formatter}]
  (let [results (let [get-models #(get-entity-manager (:database env))]
                  (finder get-models))
        props (if (some? props)
                props
                (keys (first results)))]
    {:provides  props
     :result    (map
                  #(select-keys % props)
                  results)
     :formatter formatter}))

(defn list-alphabet-letters
  []
  (adapt-results {:props     [:letter]
                  :finder    (fn [_] (get-alphabet))
                  :formatter (get-letter-formatter :letter)}))

(defn list-aggregates
  [& [finder]]
  (let [finder (fn [get-models]
                 (get-models)
                 (if (some? finder)
                   (finder)
                   (find-all-aggregates)))]
    (adapt-results {:finder    finder
                    :formatter (get-aggregate-formatter)})))

(defn list-aggregates-containing-member
  [screen-name]
  (list-aggregates #(find-aggregates-enlisting-member screen-name)))

(defn list-keyword-aggregates
  []
  (list-aggregates (fn []
                     (sort-by #(:aggregate-name %) (find-keyword-aggregates)))))

(defn list-keywords-by-aggregate
  [aggregate-name & [finder]]
  (let [finder (fn [get-models]
                 (let [_ (get-models)
                       finder (if (some? finder)
                                finder
                                find-keywords-by-aggregate-name)]
                   (reverse (finder aggregate-name))))]
    (adapt-results {:props     [:occurrences :keyword :aggregate-name :aggregate-id]
                    :finder    finder
                    :formatter (get-keyword-formatter :keyword :occurrences)})))

(defn list-highlights-since-a-month-ago
  []
  (let [finder (fn [get-models]
                 (let [_ (get-models)]
                   (find-highlights-since-a-month-ago)))]
    (adapt-results {:props     [:aggregate-id :aggregate-name :screen-name
                                :retweets :created-at
                                :status-url :text]
                    :finder    finder
                    :formatter (get-highlight-formatter
                                 :aggregate-id
                                 :aggregate-name
                                 :retweets
                                 :created-at
                                 :screen-name
                                 :status-url
                                 :text)})))

(defn list-mentions-by-aggregate
  [aggregate-name]
  (list-keywords-by-aggregate aggregate-name find-mentions-by-aggregate-name))

(defn list-member-statuses
  [screen-name]
  (adapt-results {:props     [:status-id :status-twitter-id :text :created-at :screen-name]
                  :finder    (fn [get-models]
                               (let [{status-model :status} (get-models)]
                                 (reverse (find-statuses-by-screen-name screen-name status-model))))
                  :formatter (get-status-formatter :screen-name :text :created-at)}))

(defn list-aggregate-statuses
  [aggregate-name]
  (adapt-results {:props     [:status-id :status-twitter-id :text :created-at :screen-name :aggregate-name]
                  :finder    (fn [get-models]
                               (reverse (find-statuses-by-aggregate-name
                                          aggregate-name
                                          (get-models))))
                  :formatter (get-status-formatter :screen-name :text :created-at)}))

(defn list-statuses-containing-keyword
  [keyword]
  (adapt-results {:props     [:status-id :status-twitter-id :text :created-at :screen-name :aggregate-name]
                  :finder    (fn [get-models]
                               (reverse (find-statuses-containing-keyword
                                          keyword
                                          (get-models))))
                  :formatter (get-status-formatter :screen-name :text :created-at)}))

(defn list-members
  [finder]
  (adapt-results {:props     [:aggregate-name :screen-name :member-id :member-twitter-id]
                  :finder    (fn [get-models]
                               (let [_ (get-models)]
                                 (finder)))
                  :formatter (get-member-formatter :screen-name :member-twitter-id)}))

(defn list-members-in-aggregate
  [aggregate-name]
  (list-members #(find-members-by-aggregate aggregate-name)))

(defn list-members-which-subscriptions-have-been-collected
  []
  (list-members #(find-members-which-subscriptions-have-been-collected)))

(defn list-aggregates-of-subscriber-having-screen-name
  [screen-name]
  (adapt-results {:props     [:list-name :list-twitter-id]
                  :finder    (fn [get-models]
                               (let [_ (get-models)]
                                 (find-lists-of-subscriber-having-screen-name screen-name)))
                  :formatter (get-indexed-prop-formatter :list-name :list-twitter-id)}))

(defn list-members-subscribing-to-lists
  []
  (list-members #(find-members-subscribing-to-lists)))

(defn list-subscriptions-of-member-having-screen-name
  [screen-name]
  (list-members #(find-subscriptions-of-member-having-screen-name screen-name)))

(defn get-member-description
  [screen-name]
  (adapt-results {:props     [:description :screen-name :member-id]
                  :finder    (fn [get-models]
                               (let [{member-model :members} (get-models)]
                                 (find-member-by-screen-name screen-name member-model)))
                  :formatter (get-member-formatter :screen-name :member-twitter-id)}))

(defn render-latest-result-map
  [args]
  (let [coll (if (and
                   (some? args)
                   (pos? (count args)))
               (:result (first args))
               '())
        formatter (if (some? coll)
                    (fn [m]
                      (str
                        (clojure.string/join
                          "\n"
                          (map
                            #(str (name %) ": " (get m %))
                            (sort (keys (first coll))))
                          )
                        "\n"))
                    identity)]
    (print-formatted-string
      formatter
      coll
      {:no-wrap false
       :sep     "------------------"})
    (if (some? coll)
      args
      {:result '()})))