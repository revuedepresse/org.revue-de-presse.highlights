; @see https://clojure.org/reference/compilation#_gen_class_examples
; about gen-class examples
;
(ns review.core
  (:require [clojure.edn :as edn]
            [environ.core :refer [env]]
            [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.consumers :as lc]
            [php_clj.core :refer [php->clj clj->php]]
            [clojure.data.json :as json])
  (:use [korma.core]
        [korma.db])
  (:gen-class))

(defn connect-to-db
  [config]
  (defdb database-connection {
    :classname "com.mysql.jdbc.Driver"
    :subprotocol "mysql"
    :subname (str "//" (:host config) ":" (:port config) "/" (:name config))
    :useUnicode "yes"
    :characterEncoding (:charset config)
    :delimiters "`"
    :user (:user config)
    :password (:password config)})
  (declare users)
  (defentity users
         (pk :usr_id)
         (table :weaving_user)
         (database database-connection)
         (entity-fields :usr_twitter_username :usr_twitter_id))
  ;(println (exec-raw ["SELECT * FROM weaving_user LIMIT 10"] :results))
  {:users users})

(defn find-user-by-username
  [username users]
  (println type username)
  (println type users)
  (println (-> (select* users) (where {:usr_twitter_username username}) (as-sql))))

; @see http://clojurerabbitmq.info/articles/getting_started.html#hello-world-example
; @see https://github.com/mudge/php-clj#usage
;
(defn get-message-handler
  [users]
  (fn
    [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
    (find-user-by-username (first (json/read-str (php->clj (String. payload  "UTF-8")))) users)))

(defn -main
  [& args]
  (def rabbitmq (edn/read-string (:rabbitmq env)))
  (let [conn (rmq/connect {:host (:host rabbitmq)
                           :username (:user rabbitmq)
                           :password (:password rabbitmq)
                           :port (Integer/parseInt (:port rabbitmq))
                           :vhost (:vhost rabbitmq)})
        ch   (lch/open conn)
        qname (:queue rabbitmq)]
        (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))

    (def entities (connect-to-db (edn/read-string (:database env))))
    (def message-handler (get-message-handler (:users entities)));
    (lc/subscribe ch qname message-handler {:auto-ack false})


    (Thread/sleep 60000)
    (println "[main] Disconnecting...")
    (rmq/close ch)
    (rmq/close conn)))
