(ns repository.database-schema
  (:require [korma.core :as db])
  (:use [korma.db]))

(defn get-column
  [column-name model]
  (keyword (str (:table model) "." column-name)))

(defn get-collation
  []
  " COLLATE utf8mb4_unicode_ci ")
