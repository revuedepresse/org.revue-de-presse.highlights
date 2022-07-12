(defproject highlights "1.0.2"

  :description "Command-line application for updating tweets properties collected by calling Twitter API."

  :url "https://github.com/revuedepresse/highlights.revue-de-presse.org"

  :license {:name "AGPL-3.0"
            :url "https://opensource.org/licenses/AGPL-3.0"}


  :dependencies [[org.clojure/clojure "1.11.1"]
                 [clj-time "0.15.2"]
                 [pandect "1.0.2"]
                 [environ "1.2.0"]
                 [com.novemberain/langohr "5.4.0" :exclusions [org.slf4j/slf4j-api]]
                 [org.postgresql/postgresql "42.4.0"]
                 [korma "0.4.3"]
                 [org.clojure/data.json "2.4.0"]
                 [twitter-api "1.8.0"]
                 [danlentz/clj-uuid "0.1.9"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.clojure/math.numeric-tower "0.0.5"]
                 [org.slf4j/slf4j-api "1.7.36"]
                 [org.slf4j/slf4j-reload4j "1.7.36"]
                 [php-clj "0.4.1"]
                 [org.slf4j/slf4j-simple "1.7.36" :exclusions [org.slf4j/slf4j-api]]]

  :plugins [[lein-environ "1.2.0"]]

  :aot :all

  :resource-paths ["resources"]

  :main highlights.core)
