(ns formatting.formatter)

(defn get-indexed-prop-formatter
  [prop index]
  (fn [m]
    (str
      (get m prop)
      " (#"
      (get m index)
      ")")))

(defn get-status-formatter
  [screen-name text publication-date]
  (fn [m]
    (str
      "@" (get m screen-name) ": "
      (get m text)
      " (at "
      (get m publication-date)
      ")")))