(ns formatting.formatter
  (:use [clojure.string :as string]))

(defn right-padding
  [s & args]
  (let [padding-length (if (some? args)
                         (first args)
                         25)]
    (format (str "%-" padding-length "s") s)))

(defn get-indexed-prop-formatter
  [prop index]
  (fn [m]
    (str
      (get m prop)
      " (#"
      (get m index)
      ")")))

(defn get-keyword-formatter
  [keyword occurrences]
  (fn [m]
    (let [formatter #(str
                       (get % keyword)
                       " (occurrences: "
                       (get % occurrences)
                       ")")]
      (right-padding (formatter m) 35))))

(defn get-highlight-formatter
  [aggregate-id aggregate-name retweets created-at screen-name status-link text]
  (fn [m]
    (let [formatter #(str
                       "From \""
                       (get % aggregate-name)
                       "\" (#"
                       (get % aggregate-id)
                       "), authored by @"
                       (get % screen-name)
                       " on the \"" (get % created-at) "\" "
                       "with " (get % retweets) " retweets"
                       "\n" (get % text)
                       "\n" (get % status-link))]
      (right-padding (formatter m) 200))))

(defn get-status-formatter
  [screen-name text publication-date]
  (fn [m]
    (str
      "@" (get m screen-name) ": "
      (get m text)
      " (at "
      (get m publication-date)
      ")")))

(defn get-aggregate-formatter
  []
  (let [formatter (get-indexed-prop-formatter :aggregate-name :aggregate-id)]
    (fn [m]
      (right-padding (formatter m) 35))))

(defn get-member-formatter
  [screen-name member-twitter-id]
  (let [formatter #(str
                     "@"
                     (get % screen-name)
                     " (#"
                     (get % member-twitter-id)
                     ")")]
    (fn [m]
      (right-padding (formatter m) 40))))

(defn format-selection
  [{formatter :formatter
    i         :index
    k         :key
    m         :map
    t         :total-items}]
  (str
    (right-padding
      (str i ")")
      (if (> t 999) 9 5))
    (if formatter
      (formatter m)
      (right-padding (get m k)))))

(defn print-formatted-string
  [formatter coll & options]
  (let [no-wrap (when (some? options)
                  (:no-wrap (first options)))
        item-separator (if (and
                             (some? options)
                             (:sep (first options)))
                         (:sep (first options))
                         "|")
        start-at (if (and
                       (some? options)
                       (:start-at (first options)))
                   (:start-at (first options))
                   0)
        items-per-row (if (and
                            (some? options)
                            (:items-per-row options))
                        (:items-per-row (first options))
                        5)
        effect (if no-wrap
                 identity
                 println)
        apply-effect (if no-wrap
                       #(do
                          (let [sep (if (= start-at %1) "" item-separator)
                                prefix (if (= 0 (mod %1 items-per-row))
                                         (str
                                           (if (= start-at %1)
                                             item-separator
                                             "")
                                           (if (not= start-at %1)
                                             "\n"
                                             "") sep))]
                            (effect (str prefix (formatter %2)))))
                       #(effect (formatter %2)))
        res (doall
              (map-indexed
                apply-effect
                coll))]
    (when no-wrap
      (println (str (string/join item-separator res) item-separator)))))
