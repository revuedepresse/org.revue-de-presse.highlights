(ns command.navigation
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [clojure.string :as string]
            [utils.error-handler :as error-handler])
  (:use [formatting.formatter]
        [repository.entity-manager]
        [repository.keyword]
        [repository.status]
        [repository.status-aggregate]
        [repository.aggregate]))

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
      #(string/starts-with? % prefix)
      (keys (ns-publics namespace)))
    (filter
      #(meta-pred
         (get-meta-for-command-in-namespace % "devobs.core")))
    (sort)
    (map-indexed
      #(let [cmd-name (string/replace %2 prefix "")
             cmd-index (inc %1)]
         {:name  cmd-name
          :index cmd-index}))))

(defn is-valid-numeric
  [subject]
  (re-find #"^\d+$" subject))

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
  (not (is-valid-choice input total-choices)))

(defn is-valid-command-index
  [input total-commands]
  (is-valid-choice input total-commands))

(defn print-menu-when
  [pred]
  (when pred
    (do
      (println "\nEntering interactive mode")
      (println "\n[h]elp about available commands")
      (println "[q]uit or CTRL/COMMAND + C"))))

(defn get-requirements
  [f]
  (:requires (meta f)))

(defn has-requirements?
  [f]
  (seq (get-requirements f)))

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
  [f next coll & [formatter]]
  (let [requirements (get-requirements f)
        next-eval (when (has-single-requirement? f)
                    (if formatter
                      (next (first requirements) coll formatter)
                      (next (first requirements) coll)))]
    next-eval))

(defn transform-coll
  [f f-key single-requirement coll & [formatter]]
  (let [choices (map-indexed
                  #(f {:formatter   formatter
                       :key         single-requirement
                       :index       (f-key %1)
                       :map         %2
                       :total-items (count coll)})
                  coll)]
    choices))

(defn prompt-choices
  [single-requirement coll & [formatter]]
  (let [formatter (when (some? formatter)
                    formatter)
        printable-choices (transform-coll format-selection #(inc %) single-requirement coll formatter)]
    (when (some? printable-choices)
      (println (str "Please select one " (string/replace (name single-requirement) "-" " ")))
      (print-formatted-string
        identity
        printable-choices
        {:no-wrap true}))))

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

(defn validate-input
  [input choices]
  (loop [choice-candidate input]
    (if (and
          (is-invalid-choice choice-candidate (count choices))
          (not (should-quit choice-candidate)))
      (do
        (println "Please select a valid choice (or [q]uit).")
        (recur (read-line)))
      choice-candidate)))

(defn let-user-make-a-choice
  [f result-map]
  (let [coll (:result result-map)
        formatter (:formatter result-map)
        _ (when-f-has-single-requirement
            f
            prompt-choices
            coll
            formatter)
        choices (when-f-has-single-requirement
                  f
                  get-choices
                  coll)
        input (read-line)
        valid-choice (validate-input input choices)]
    {:user-choice valid-choice
     :choices     choices}))

(defn run-command-indexed-at
  [index ns-commands result-map]
  (let [command (:name (nth
                         ns-commands
                         (dec index)))
        f (resolve (symbol (str "devobs.core/command-" command)))]
    (cond
      (and
        (has-requirements? f)
        (not (meets-any-requirements? f))) (let [{user-choice :user-choice
                                                  choices     :choices} (let-user-make-a-choice f result-map)]
                                             (if (should-quit user-choice)
                                               {:result "q"}
                                               (apply-with f command choices user-choice)))
      (meets-any-requirements? f) (apply f [result-map])
      :else (do
              (print-command-name command) 1
              (apply f [])))))

(defn find-ns-symbols-without-args
  []
  (find-ns-symbols-having-prefix
    'devobs.core
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
    'devobs.core
    "command-"
    #(are-requirements-fulfilled? (set (:requires %)) (set provided-args))))

(defn get-formatted-string-printer
  [formatter coll]
  (fn []
    (print-formatted-string formatter coll)))

(defn format-command
  [command]
  (str
    (right-padding (str (:index command) ")") 4)
    (right-padding (:name command))))

(defn print-new-line
  [] (println "\n"))

(defn try-running-command
  [f args]
  (try
    (apply f [args])
    (catch Exception e
      (error-handler/log-error e))))

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

(defn handle-input
  [result-map]
  (let [ns-commands (find-ns-symbols result-map)
        input (if (should-quit-from-last-result result-map)
                "q"
                (read-line))]
    (cond
      (= input "q") (do
                      (println "bye"))
      (= input "h") (do
                      (print-help ns-commands)
                      [false nil])
      (is-valid-command-index input (count ns-commands)) [false (run-command-indexed-at
                                                                  (Long/parseLong input)
                                                                  ns-commands
                                                                  result-map)]
      :else (do
              (println (str "\nInvalid command: \"" input "\""))
              [false result-map]))))
