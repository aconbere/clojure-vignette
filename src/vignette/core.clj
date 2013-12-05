(ns vignette.core
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [vignette.db :as vdb]
            [vignette.client :as client]
            [clojure.math.numeric-tower :as math :refer [floor]]
            [clojure.set :refer [difference]]
            ))

(defn key->host
  [neighbor-key]
  (client/string->host (subs neighbor-key 2)))

(defn find-neighbors
  [db]
  (set (map key->host (keys (vdb/search db "n:%")))))

(defn pick-neighbors
  [db n filtered-hosts]
  (let [neighbors (difference (find-neighbors db) filtered-hosts)]
    (if (empty? neighbors)
      nil
      (take n (shuffle neighbors)))))

(defn store-neighbor
  [db host]
  (let [t (int (floor (/ (/ (System/currentTimeMillis) 60) 1000)))
        k (str "n:" (client/host->string host))
        v { 0 t }
        db (first (vdb/update db k v))]
    db))

(defn store-neighbors
  [db hosts]
  (reduce (fn [db h] (store-neighbor db h)) {} hosts))

(defn is-search? [k] (re-matches #".*%.*" k))
(defn is-aggregate? [k] (re-matches #".*\*.*" k))

(defn message-type
  [{ k "key" v "vector" ttl "ttl"}]
  (cond
    (is-aggregate? k) :aggregate
    (is-search? k) :search
    :else :store))

(defmulti handle-message (fn [db out server msg datagram] (message-type msg)))

(defmethod handle-message :store
  [db out server { k "key" v "vector" ttl "ttl"} from ]

  (when (not (contains? db k))
    (println "key not found")
    (client/query-neighbors (pick-neighbors db 3 #{server}) out k))

  (let [[db updates] (vdb/update db k v)]
    (if (not-empty updates)
      (let [neighbors (pick-neighbors db 1 #{server (client/datagram->host from)})]
        (client/send-updates neighbors out k updates (- 1 ttl))))
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
            to-send (client/udp-msg from {"key" k "vector" v "ttl" 50})]
          (println to-send)
          (go (>! out to-send))))
  db))

(defn run-server [port]
  (let [[in out] (client/make-client port)
        server {:host "127.0.0.1" :port port}
        db (store-neighbor {} server)]
    (go-loop [db db
              datagram (<! in)]
      (let [msg (client/datagram->message datagram)]
        (println "received" msg "from" (:host datagram) (:port datagram))
        (if (> (get msg "ttl" 0) 0)
          (let [db (-> db
                       (store-neighbor datagram)
                       (handle-message out server msg datagram))]
            (recur db (<! in)))
          (recur db (<! in)))))
    [in out]))

(defn -main [port & neighbors]
  (println "Starting vignette node on port:" port "with neighbors: " neighbors)
  (let [[in out] (run-server (client/parse-int port))
        neighbors (map #(client/string->host %) neighbors)]
    (client/query-neighbors neighbors out "n:%")))
