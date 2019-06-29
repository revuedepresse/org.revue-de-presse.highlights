(ns command.navigation
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [clojure.string :as string])
  (:use [repository.entity-manager]
        [repository.aggregate]))

(defn list-aggregates
  []
  (let [_ (get-entity-manager (:database env))
        aggregates (find-all-aggregates)]
    {:provides  [:aggregate-name :aggregate-id]
     :result    aggregates
     :formatter #(str
                   (:aggregate-name %)
                   " (#"
                   (:aggregate-id %)
                   ")")}))

(defn list-members-in-aggregate
  [aggregate-name]
  (let [_ (get-entity-manager (:database env))
        members (find-members-by-aggregate aggregate-name)]
    {:provides  [:aggregate-name :screen-name :member-id :member-twitter-id]
     :result    members
     :formatter #(str
                   (:screen-name %)
                   " (#"
                   (:member-twitter-id %)
                   ")")}))

(defn get-meta-for-command-in-namespace
  [command namespace]
  (meta
    (resolve
      (symbol
        (str namespace "/" command)))))

(defn find-ns-symbols-having-prefix
  [namespace prefix meta-pred]
  (->>
    (filter
      #(string/starts-with? % "command-")
      (keys (ns-publics namespace)))
    (filter
      #(meta-pred
         (get-meta-for-command-in-namespace % "review.core")))
    (sort)
    (map-indexed
      #(let [cmd-name (string/replace %2 prefix "")
             cmd-index (inc %1)]
         {:name  cmd-name
          :index cmd-index}))))

(defn is-valid-numeric
  [subject]
  (re-find #"^\d$" subject))

(defn long-from-numeric
  [numeric]
  (when numeric
    (Long/parseLong numeric)))

(defn is-valid-choice
  [input total-choices]
  (let [matching-integer (is-valid-numeric input)
        choice-index (long-from-numeric matching-integer)]
    (and
      (some? choice-index)
      (< choice-index total-choices))))

(defn is-invalid-choice
  [input total-choices]
  (nil? (is-valid-choice input total-choices)))

(defn is-valid-command-index
  [input total-commands]
  (is-valid-choice input total-commands))

(defn print-menu-when
  [pred]
  (when pred
    (do
      (println "\nEntering interactive mode")
      (println "\n\"q\" to quit or CTRL/COMMAND + C")
      (println "\"h\" to list available commands"))))

(defn get-requirements
  [f]
  (:requires (meta f)))

(defn has-requirements?
  [f]
  (seq (get-requirements f)))

(defn right-padding
  [s & args]
  (let [padding-length (if (some? args)
                         (first args)
                         25)]
    (format (str "%-" padding-length "s") s)))

(defn format-selection
  [{m :map
    k :key
    i :index}]
  (str
    (right-padding (str i ")") 5)
    (right-padding (get m k))))

(defn print-formatted-string
  [formatter coll & options]
  (let [no-wrap (when (some? options)
                  (:no-wrap (first options)))
        item-separator (if (and
                             (some? options)
                             (:sep (first options)))
                         (:sep (first options))
                         "|")
        items-per-row (if (and
                            (some? options)
                            (:items-per-row options))
                        (:items-per-row (first options))
                        5)
        divisor (inc items-per-row)
        effect (if no-wrap
                 identity
                 println)
        apply-effect (if no-wrap
                       #(do
                          (let [sep (if (= 0 %1) "" item-separator)
                                prefix (if (= 0 (mod (inc %1) divisor)) (str item-separator "\n") sep)]
                            (effect (str prefix (formatter %2)))))
                       #(effect (formatter %2)))
        res (doall
              (map-indexed
                apply-effect
                coll))]
    (when no-wrap
      (println (str (string/join "" res) "|")))))

(defn get-choice
  [{choice             :map
    i                  :index
    single-requirement :key}]
  (let [requirement (get choice single-requirement)]
    {single-requirement requirement
     :index             i}))

(defn has-single-requirement?
  [f]
  (let [requirements (get-requirements f)]
    (= 1 (count requirements))))

(defn when-f-has-single-requirement
  [f next coll]
  (let [requirements (get-requirements f)
        next-eval (when (has-single-requirement? f)
                    (next (first requirements) coll))]
    next-eval))

(defn transform-coll
  [f f-key single-requirement coll]
  (let [choices (map-indexed
                  #(f {:key   single-requirement
                       :map   %2
                       :index (f-key %1)})
                  coll)]
    choices))

(defn prompt-choices
  [single-requirement coll]
  (let [printable-choices (transform-coll format-selection #(inc %) single-requirement coll)]
    (when (some? printable-choices)
      (println (str "Please select one " (string/replace (name single-requirement) "-" " ")))
      (print-formatted-string identity printable-choices {:no-wrap true}))))

(defn get-choices
  [single-requirement coll]
  (let [choices (transform-coll get-choice #(inc %) single-requirement coll)]
    (if (some? choices)
      choices
      '())))

(defn print-command-name
  [command & [args]]
  (let [first-argument (when (some? args)
                         (first args))]
    (if first-argument
      (println
        (str
          "\nAbout to run command \"" command
          "\" with \"" first-argument "\" passed as first argument"))
      (println
        (str
          "\nAbout to run command \"" command
          "\"")))))

(defn apply-with
  [f command choices valid-choice]
  (let [selected-choice (dec (Long/parseLong valid-choice))
        first-requirement (first (get-requirements f))
        args [(get (nth choices selected-choice) first-requirement)]]
    (print-command-name command args)
    (apply f [args])))

(defn should-quit-from-last-result
  [result]
  (= "q" (:result result)))

(defn should-quit
  [input]
  (= input "q"))

(defn meets-any-requirements?
  [f]
  (and
    (has-requirements? f)
    (= :any (first (get-requirements f)))))

(defn run-command-indexed-at
  [index ns-commands last-eval]
  (let [command (:name (nth
                         ns-commands
                         (dec index)))
        f (resolve (symbol (str "review.core/command-" command)))]
    (cond
      (and
        (has-requirements? f)
        (not (meets-any-requirements? f))) (let [coll (:result last-eval)
                                                 _ (when-f-has-single-requirement
                                                     f
                                                     prompt-choices
                                                     coll)
                                                 choices (when-f-has-single-requirement
                                                           f
                                                           get-choices
                                                           coll)
                                                 input (read-line)
                                                 valid-choice (loop [choice-candidate input]
                                                                (if (and
                                                                      (is-invalid-choice choice-candidate (count choices))
                                                                      (not (should-quit input)))
                                                                  (do
                                                                    (println "Please select a valid choice.")
                                                                    (recur (read-line)))
                                                                  choice-candidate))]
                                             (if (should-quit input)
                                               {:result "q"}
                                               (apply-with f command choices valid-choice)))
      (meets-any-requirements? f) (apply f [last-eval])
      :else (do
              (print-command-name command) 1
              (apply f [])))))

(defn find-ns-symbols-without-args
  []
  (find-ns-symbols-having-prefix
    'review.core
    "command-"
    #(zero?
       (count
         (first
           (:arglists %))))))

(defn are-requirements-fulfilled?
  [requirements provided-args]
  (when
    (pos? (count requirements))
    (clojure.set/subset? requirements provided-args)))

(defn find-ns-symbols-requiring
  [provided-args]
  (find-ns-symbols-having-prefix
    'review.core
    "command-"
    #(are-requirements-fulfilled? (set (:requires %)) (set provided-args))))

(defn get-formatted-string-printer
  [formatter coll]
  (fn []
    (print-formatted-string formatter coll)))

(defn format-command
  [command]
  (str
    (right-padding (str (:index command) ")") 3)
    (right-padding (:name command))))

(defn print-new-line
  [] (println "\n"))

(defn try-running-command
  [f args]
  (try
    (apply f [args])
    (catch Exception e
      (log/error (.getMessage e)))))

(defn find-ns-symbols-from
  [last-eval]
  (if (and
        (some? last-eval)
        (:provides last-eval))
    (find-ns-symbols-requiring (:provides last-eval))
    '()))

(defn find-ns-symbols
  [last-eval]
  (let [ns-commands-available-from-last-eval (find-ns-symbols-from last-eval)
        show-last-evaluation (if (some? last-eval)
                               (apply list [{:name "show-latest-evaluation"}])
                               '())
        ns-commands (->>
                      (into
                        (find-ns-symbols-without-args)
                        ns-commands-available-from-last-eval)
                      (into
                        show-last-evaluation)
                      (map
                        #(dissoc % :index))
                      (sort-by
                        #(:name %))
                      (map-indexed
                        #(assoc %2 :index (inc %1))))]
    (when
      (pos?
        (count ns-commands-available-from-last-eval))
      (do
        (print-new-line)
        (print-formatted-string
          #(format-command %)
          ns-commands)))
    ns-commands))

(defn print-help
  [ns-commands]
  (let [show-available-commands (get-formatted-string-printer
                                  #(format-command %)
                                  ns-commands)]
    (show-available-commands)))
