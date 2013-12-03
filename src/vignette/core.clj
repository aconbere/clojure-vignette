(ns vignette.core
  (:require [ac.udp :as udp :refer [udp-socket]]
            [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [clj-msgpack.core :as mp :refer [pack unpack]]
            [vignette.db :as vdb]
            [clojure.math.numeric-tower :as math :refer [floor]]
            [clojure.string :as string :refer [split]]
            ))

(defn- parse-int [s]
  (Integer/parseInt (re-find #"\A-?\d+" s)))

(defn parse-host-string
  [host-string]
  (let [[host port] (take 2 (split (subs host-string 2) #":"))]
    [host (parse-int port)]))

(defn find-neighbors
  [db]
  (distinct (map parse-host-string (keys (vdb/search db "n:%")))))

(defn pick-neighbor
  [db my-host my-port]
  (let [me (list my-host my-port)
        neighbors (find-neighbors db)]
    (rand-nth (filter #(not (= me %)) neighbors ))))

(defn store-neighbor
  [db host port]
    (let [t (int (floor (/ (/ (System/currentTimeMillis) 60) 1000)))
          k (str "n:" host ":" port)
          v { 0 t }]
      (first (vdb/update db k v))))

(defn udp-msg
  [host port msg]
  {:host host :port port :message (mp/pack msg)})

(defn is-search? [k] (re-matches #".*%.*" k))
(defn is-aggregate? [k] (re-matches #".*\*.*" k))

(defn message-type
  [{ k "key" v "vector" ttl "ttl"}]
  (cond
    (is-aggregate? k) :aggregate
    (is-search? k) :search
    :else :store))

(defmulti handle-message (fn [out server db msg datagram] (message-type msg)))

(defmethod handle-message :store
  [out server db { k "key" v "vector" ttl "ttl"} _ ]
  (let [[db updates] (vdb/update db k v)]
    (if (not-empty updates)
      (let [[host port] (pick-neighbor db (:host server) (:port server))
            to-send (udp-msg host port {:key k :vector updates :ttl (- ttl 1)})]
        (go (>! out to-send))))
    db))

(defmethod handle-message :aggregate
  [out server db msg datagram]
  db)

(defmethod handle-message :search
  [out server db { query "key" v "vector" ttl "ttl"} { host :host port :port }]
  (println "searching db with query:" query)
  (let [results (vdb/search db query)]
    (println "found:" results)
    (println "keys:" (keys results))

    (doseq [k (keys results)]
      (let [v (results k)
            to-send (udp-msg host port {:key k :vector v :ttl 50})]
          (println to-send)
          (go (>! out to-send))))
  db))

(defn grab-message
  "Get and parse a message off of a datagram"
  [{ host :host port :port message :message }]
    (let [barray (.array message)]
      (first (unpack barray))))

(defn run-server [port]
  (let [[in out] (udp-socket {:port port})
        server { :host "127.0.0.1" :port port }
        db (store-neighbor {} (:host server) (:port server))]
    (go-loop [db db, datagram (<! in)]
      (let [msg (grab-message datagram)]
        (println "received" msg "from" (:host datagram) (:port datagram))
        (let [db (handle-message out server db msg datagram)
              db (store-neighbor db (:host datagram) (:port datagram))]
          (recur db (<! in)))))))

(defn -main [port]
  (println "Starting vignette node on port:" port)
  (run-server (parse-int port)))

