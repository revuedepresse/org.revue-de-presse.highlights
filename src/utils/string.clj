(ns utils.string)

(defn explode
  [sep subject]
  (apply vector (clojure.string/split subject sep)))
