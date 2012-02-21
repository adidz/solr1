(ns publicearth.rabbit
  (:use [publicearth.place :as place :only ()]
        [publicearth.search :as search :only ()]
        [publicearth.mail :as mail :only ()]
        [clojure.contrib.logging :as log :only ()]
        clojure.contrib.str-utils))

(import '(com.rabbitmq.client ConnectionParameters ConnectionFactory Channel Consumer QueueingConsumer)
        '(java.io IOException)
        '(java.sql Timestamp)
        '(java.util Date))

(def *noack* false)

(defn connect [config]
  "Connect to the RabbitMQ server"
  (let [params (ConnectionParameters.)]
    (doto params
      (.setUsername (or (config :user) "guest"))
      (.setPassword (config :password))
      (.setVirtualHost (or (config :vhost) "/"))
      (.setRequestedHeartbeat 0))
    (try
      (.newConnection (ConnectionFactory. params) (or (config :host) "localhost") (or (Integer. (config :port)) 5672))
      (catch IOException e (.printStackTrace e)))))

(defn get-channel [config]
  (let [channel (.createChannel (connect config))]
    (doto channel
      (.exchangeDeclare (config :exchange) "direct" true)
      (.queueDeclare (config :queue) true)
      (.queueBind (config :queue) (config :exchange) (config :key)))
    channel))

(defn get-consumer [channel]
  (QueueingConsumer. channel))

(defn has-timestamp?
  "Is the given field a valid timestamp?"
  [ts]
  (if (or (nil? ts) (= ts ""))
    false
    (try
      (Integer/parseInt ts)
      true
      (catch Exception _ false))))

(defn listen [config]
  "Listen to RabbitMQ and handle any requests"
  (let [channel (get-channel config)
        consumer (get-consumer channel)]
    (.basicConsume channel (config :queue) *noack* (config :key) consumer)
    (loop []
      (let [delivery (.nextDelivery consumer)
            id_ts (String. (.getBody delivery))]
        ; break the ID and timestamp apart
        (let [[id, ts] (re-split #"\s" id_ts)
              timestamp (if (has-timestamp? ts) (Timestamp. (* (Integer. ts) 1000)))]
          (log/debug (str "Indexing place " id " at " timestamp))
          (try
            (loop [tries 5]
              (let [place (try (place/get-place id) (catch Throwable e (log/error (str "Couldn't get place " id " - " (.getMessage e)))))]
                (if (and place (or (nil? timestamp) (< (.compareTo timestamp (or (place :updated_at) (place :created_at))) 0)))
                  ; only update the place in the search index if the timestamp passed in occurs after
                  ; the one in the database
                  (do
                    (log/info (str "Received request to reindex place " id))
                    (search/post place)
                    (.basicAck channel (.getDeliveryTag (.getEnvelope delivery)) false))

                  ; otherwise, wait two seconds and try again
                  (if (> tries 0)
                    (do
                      (log/info (str "Waiting 5 seconds on place " id))
                      (Thread/sleep 5000)
                      (recur (dec tries)))
                    (do
                      (throw (Exception. (str "Failed to index place " id))))))))
            (catch Throwable e
              (try
                (mail/send-message
                  :user "exception.user@publicearth.com"
                  :password "qwer1234"
                  :host "smtp.gmail.com"
                  :port 465
                  :ssl true
                  :to ["exceptions@publicearth.com" ]
                  :subject (str "[INDEXER] Failed to index place " id ".")
                  :text (str (.getMessage e) "\n\n" (str-join "\n" (.getStackTrace e))))
              (catch Throwable e (log/error (str "Failed to send mail.  Message failed to index: " id "\n"
                  (.getMessage e) "\n\n" (str-join "\n" (.getStackTrace e))))))
              (.basicAck channel (.getDeliveryTag (.getEnvelope delivery)) false)))))
      (recur))))
