(ns publicearth.app
  (:gen-class)
  (:use [publicearth.place :as place :only ()]
        [publicearth.search :as search :only ()]
        [publicearth.rabbit :as rabbit :only ()]
        [clojure.contrib.logging :as log :only ()]
        clojure.contrib.java-utils))

(import '(java.util Properties Date)
        '(java.io File))

(def *env* (System/getProperty "app.env" "development"))

(defn current-environment [] *env*)

(defn local-v-etc [filename]
  "Look in /etc for the filename, otherwise it's local to the current directory"
  (if (.exists (File. (str "/etc/" filename))) (str "/etc/" filename) filename))

(defn prop-key [key] 
  "Prepend the environment label to the property key for a configuration file."
  (str *env* "." key))

(defn configure-db [db]
  "Load the properties to configure the database from the configuration file."
  {
   :host (.getProperty db (prop-key "db.host"))
   :name (.getProperty db (prop-key "db.name"))
   :port (.getProperty db (prop-key "db.port"))
   :user (.getProperty db (prop-key "db.user"))
   :password (.getProperty db (prop-key "db.password"))
  })

(defn configure-solr [solr]
  "Load the properties to configure the Solr server from the configuration file."
  {
   :host (.getProperty solr (prop-key "search.host"))
   :port (.getProperty solr (prop-key "search.port"))
   :context (.getProperty solr (prop-key "search.context"))
   :queue-size (Integer. (.getProperty solr (prop-key "search.queue")))
   :threads (Integer. (.getProperty solr (prop-key "search.threads")))
  })

(defn configure-rabbit [rabbit]
  "Load the properties to configure the Rabbit server from the configuration file."
  {
   :host (.getProperty rabbit (prop-key "rabbit.host"))
   :port (.getProperty rabbit (prop-key "rabbit.port"))
   :user (.getProperty rabbit (prop-key "rabbit.user"))
   :password (.getProperty rabbit (prop-key "rabbit.password"))
   :vhost (.getProperty rabbit (prop-key "rabbit.vhost"))
   :exchange (.getProperty rabbit (prop-key "rabbit.exchange"))
   :queue (.getProperty rabbit (prop-key "rabbit.queue"))
   :key (.getProperty rabbit (prop-key "rabbit.key"))
  })

(defn -main []
  "If you pass in a place.id property, will index that individual place.  Pass in bulk=all/date to reindex
   a number of places.  All will just reindex everything, while 'date' indicates a date you supply to
   reindex everything after that date.

   If you pass nothing in, connects to RabbitMQ and waits for places to be posted to the message queue
   (standalone server mode)."

  (let [properties (read-properties (or (System/getProperty "indexer.config") (local-v-etc "indexer.properties")))
        db (configure-db properties)
        solr (configure-solr properties)
        rabbit (configure-rabbit properties)]

    (println (str "Connected to database " (db :name) "@" (db :host)))
    (println (str "Connected to Solr " (solr :context) "@" (solr :host)))

    (place/connect db)
    (search/connect solr)

    (let [id (System/getProperty "place.id")
          bulk (System/getProperty "bulk")]

      ; TODO:  Bulk loader!!!

      (if id
        ; process an individual place into search
        (let [start (System/currentTimeMillis)]
          (let [place (place/get-place id)]
            (search/post place)
            (println (str "For " id " it took " (- (System/currentTimeMillis) start) " ms."))))

        ; otherwise listen to RabbitMQ and handle any requests
        (rabbit/listen rabbit)))))
