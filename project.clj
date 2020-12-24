(defproject devobs "0.1.0"

  :description "Easing observation of statuses from Twitter lists related to software development"

  :url "https://github.com/thierrymarianne/devobs-worker"

  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clj-time "0.15.0"]
                 [pandect "0.6.1"]
                 [environ "1.1.0"]
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

  :aot :all

  :resource-paths ["resources"]

  :main devobs.core)
