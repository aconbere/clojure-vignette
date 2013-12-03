(ns vignette.core
  (:require [ac.udp :as udp :refer [udp-socket]]
            [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [clj-msgpack.core :as mp :refer [pack unpack]]
            [vignette.db :as vdb]
            [clojure.math.numeric-tower :as math :refer [floor]]
            [clojure.string :refer [split join]]
            [clojure.set :refer [difference]]
            ))

(defn- parse-int [s]
  (Integer/parseInt (re-find #"\A-?\d+" s)))

(defn parse-host-string
  [host-string]
  (let [[host port] (take 2 (split (subs host-string 2) #":"))]
    { :host host :port (parse-int port)}))

(defn make-host-string [ { host :host port :port }]
  (join ":" [host port]))

(defn datagram->message
  "Get and parse a message off of a datagram"
  [{ host :host port :port message :message }]
    (let [barray (.array message)]
      (first (unpack barray))))

(defn find-neighbors
  [db]
  (set (map parse-host-string (keys (vdb/search db "n:%")))))

(defn pick-neighbors
  [db n filtered-hosts]
  (let [neighbors (difference (find-neighbors db) filtered-hosts)]
    (if (empty? neighbors)
      nil
      (take n (shuffle neighbors)))))

(defn store-neighbor
  [db host]
    (let [t (int (floor (/ (/ (System/currentTimeMillis) 60) 1000)))
          k (str "n:" (make-host-string host))
          v { 0 t }]
      (first (vdb/update db k v))))

(defn udp-msg
  [{ host :host port :port} msg]
  {:host host :port port :message (mp/pack msg)})

(defn is-search? [k] (re-matches #".*%.*" k))
(defn is-aggregate? [k] (re-matches #".*\*.*" k))

(defn message-type
  [{ k "key" v "vector" ttl "ttl"}]
  (cond
    (is-aggregate? k) :aggregate
    (is-search? k) :search
    :else :store))

(defn send-updates
  [neighbors out k updates ttl]
  (doseq [neighbor neighbors]
    (let [msg (udp-msg neighbor {:key k :vector updates :ttl ttl})]
      (go (>! out msg)))))

(defmulti handle-message (fn [db out server msg datagram] (message-type msg)))

(defmethod handle-message :store
  [db out server { k "key" v "vector" ttl "ttl"} _ ]
  (let [[db updates] (vdb/update db k v)]
    (if (not-empty updates)
      (let [neighbors (pick-neighbors db 1 #{server})]
        (send-updates neighbors out k updates (- 1 ttl))))
    db))

(defmethod handle-message :aggregate
  [db out server msg datagram]
  db)

(defmethod handle-message :search
  [db out server { query "key" v "vector" ttl "ttl"} from]
  (println "searching db with query:" query)
  (let [results (vdb/search db query)]
    (doseq [k (keys results)]
      (let [v (results k)
            to-send (udp-msg from {:key k :vector v :ttl 50})]
          (println to-send)
          (go (>! out to-send))))
  db))

(defn run-server [port]
  (let [[in out] (udp-socket {:port port})
        server { :host "127.0.0.1" :port port }
        db (store-neighbor {} server)]
    (go-loop [db db
              datagram (<! in)]
      (let [msg (datagram->message datagram)]
        (println "received" msg "from" (:host datagram) (:port datagram))
        (let [db (-> db
                     (handle-message out server msg datagram)
                     (store-neighbor datagram))]
          (recur db (<! in)))))))

(defn -main [port]
  (println "Starting vignette node on port:" port)
  (run-server (parse-int port)))

