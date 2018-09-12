(defproject press-review-clojure "0.1.0"
  :description "Easing observation of Twitter lists to publish a daily press review https://twitter.com/revue_2_presse"
  :url "https://revue-de-press.weaving-the-web.org"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-http "3.9.1"]
                 [cheshire "5.8.0"]]
  :main ^:skip-aot press_review_clojure.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
