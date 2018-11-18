(ns utils.string
  (:require [clojure.string :as string]))

(defn explode
  [sep subject]
  (apply vector (clojure.string/split subject sep)))

(defn replace-underscore-with-dash
  [[k v]]
  (let [key-value [(keyword (string/replace (name k) #"-" "_") ) v]]
    key-value))

(defn snake-case-keys
  [m]
  (let [props-having-converted-keys (->> (map replace-underscore-with-dash m)
                                         (into {}))]
    props-having-converted-keys))

(defn get-prefixer
  [prefix]
  (fn [[k v]]
    (let [keyword-name (name k)
          prefixed-keyword-name (str prefix keyword-name)
          key-value [(keyword prefixed-keyword-name) v]]
      key-value)))

(defn prefixed-keys
  [m]
  (let [prefixer (get-prefixer "ust_")
        props-having-converted-keys (->> (map prefixer m)
                                         (into {}))]
    props-having-converted-keys))
