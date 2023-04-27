(defproject highlights "1.0.2"

  :description "Command-line application for updating tweets properties collected by calling Twitter API."

  :url "https://github.com/revuedepresse/highlights.revue-de-presse.org"

  :license {:name "AGPL-3.0"
            :url "https://opensource.org/licenses/AGPL-3.0"}


  :dependencies [[clj-http "3.12.3"]
                 [clj-time "0.15.2"]
                 [com.fzakaria/slf4j-timbre "0.3.21"]
                 [com.novemberain/langohr "5.4.0" :exclusions [org.slf4j/slf4j-api]]
                 [com.taoensso/timbre "5.2.1"]
                 [danlentz/clj-uuid "0.1.9"]
                 [environ "1.2.0"]
                 [korma "0.4.3"]
                 [pandect "1.0.2"]
                 [org.clojure/clojure "1.11.1"]
                 [org.clojure/data.json "2.4.0"]
                 [org.clojure/math.numeric-tower "0.0.5"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.postgresql/postgresql "42.4.0"]
                 [php-clj "0.4.1"]
                 [twitter-api "1.8.0"]]

  :plugins [[lein-environ "1.2.0"]]

  :aot :all

  :resource-paths ["resources"]

  :main highlights.core)
