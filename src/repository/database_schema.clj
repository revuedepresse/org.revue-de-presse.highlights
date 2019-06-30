(ns repository.database-schema)

(defn get-column
  [column-name model]
  (keyword (str (:table model) "." column-name)))

(defn get-collation
  []
  " COLLATE utf8mb4_unicode_ci ")