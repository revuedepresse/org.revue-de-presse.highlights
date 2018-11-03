(defproject review "0.1.0"

  :description "Easing observation of Twitter lists to publish a daily press review https://twitter.com/revue_2_presse"

  :url "https://revue-de-press.weaving-the-web.org"

  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [environ "1.1.0"]
                 [com.novemberain/langohr "5.0.0" :exclusions [org.slf4j/slf4j-api]]
                 [mysql/mysql-connector-java "5.1.47"]
                 [korma "0.4.0"]
                 [org.clojure/data.json "0.2.6"]
                 [twitter-api "1.8.0"]
                 [org.slf4j/slf4j-api "1.6.2"]
                 [org.slf4j/slf4j-log4j12 "1.6.2"]
                 [php-clj "0.4.1"]
                 [org.slf4j/slf4j-simple "1.8.0-beta2" :exclusions [org.slf4j/slf4j-api]]]

  :aot :all

  :resource-paths ["resources"]

  :main review.core)
