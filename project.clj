(defproject highlights "1.0.2"

  :description "Command-line application for updating tweets properties collected by calling Twitter API."

  :url "https://github.com/revuedepresse/highlights.revue-de-presse.org"

  :license {:name "AGPL-3.0"
            :url "https://opensource.org/licenses/AGPL-3.0"}


  :dependencies [; https://clojure.org/releases/downloads
                 [org.clojure/clojure "1.11.1"]
                 ; https://github.com/mudge/php-clj
                 [clj-time "0.15.2"]
                 ; https://github.com/xsc/pandect
                 [pandect "1.0.2"]
                 ; https://github.com/weavejester/environ
                 [environ "1.2.0"]
                 ; https://github.com/michaelklishin/langohr
                 [com.novemberain/langohr "5.4.0" :exclusions [org.slf4j/slf4j-api]]
                 ; https://search.maven.org/artifact/org.postgresql/postgresql/42.4.0/jar
                 [org.postgresql/postgresql "42.4.0"]
                 ; https://github.com/korma/Korma/tree/v0.4.3
                 [korma "0.4.3"]
                 ; https://github.com/clojure/data.json
                 [org.clojure/data.json "2.4.0"]
                 ; https://github.com/adamwynne/twitter-api
                 [twitter-api "1.8.0"]
                 ; https://danlentz.github.io/clj-uuid/
                 [danlentz/clj-uuid "0.1.9"]
                 ; https://clojure.github.io/tools.logging/
                 [org.clojure/tools.logging "1.1.0"]
                 ; https://github.com/clojure/math.numeric-tower
                 [org.clojure/math.numeric-tower "0.0.5"]
                 ; https://mvnrepository.com/artifact/org.slf4j/slf4j-api
                 [org.slf4j/slf4j-api "1.7.36"]
                 ; https://mvnrepository.com/artifact/org.slf4j/slf4j-reload4j
                 [org.slf4j/slf4j-reload4j "1.7.36"]
                 ; https://github.com/mudge/php-clj
                 [php-clj "0.4.1"]
                 ; https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
                 [org.slf4j/slf4j-simple "1.7.36" :exclusions [org.slf4j/slf4j-api]]]

  :plugins [[lein-environ "1.2.0"]]

  :aot :all

  :resource-paths ["resources"]

  :main highlights.core)
