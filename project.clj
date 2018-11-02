(defproject press-review "0.1.0"
  :description "Easing observation of Twitter lists to publish a daily press review https://twitter.com/revue_2_presse"
  :url "https://revue-de-press.weaving-the-web.org"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.9.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojurescript "1.10.339"]
                                  [com.bhauman/figwheel-main "0.1.9"]
                                  [com.bhauman/rebel-readline-cljs "0.1.4"]]
                   :resource-paths ["target"]
                   :clean-targets ^{:protect false} ["target"]} }
  :aliases {"fig" ["trampoline" "run" "-m" "figwheel.main"]}
  :paths ["cljs-src" "src" "resources" "target"]
  :source-paths ["src" "cljs-src"]
  )
