(defproject highlights "1.0.2"

  :description "Command-line application for updating tweets properties collected by calling Twitter API."

  :url "https://github.com/revuedepresse/highlights.revue-de-presse.org"

  :license {:name "AGPL-3.0"
            :url "https://opensource.org/licenses/AGPL-3.0"}

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clj-time "0.15.0"]
                 [pandect "0.6.1"]
                 [environ "1.2.0"]
                 [com.novemberain/langohr "5.0.0" :exclusions [org.slf4j/slf4j-api]]
                 [org.postgresql/postgresql "42.2.18.jre7"]
                 [korma "0.4.0"]
                 [org.clojure/data.json "0.2.6"]
                 [twitter-api "1.8.0"]
                 [danlentz/clj-uuid "0.1.7"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/math.numeric-tower "0.0.4"]
                 [org.slf4j/slf4j-api "1.6.2"]
                 [org.slf4j/slf4j-log4j12 "1.6.2"]
                 [php-clj "0.4.1"]
                 [org.slf4j/slf4j-simple "1.8.0-beta2" :exclusions [org.slf4j/slf4j-api]]]

  :plugins [[lein-environ "1.2.0"]]

  :aot :all

  :resource-paths ["resources"]

  :main highlights.core)
