(ns vignette.core
  (:use [lamina core] [aleph udp] [clj-msgpack.core :as mp] [gloss core io]))

(def db {})

(defn make-key-regex [k]
  (re-pattern (clojure.string/replace k #"%" ".*")))

(defn key-matches? [k b]
  (boolean (re-matches (make-key-regex k) b)))

(defn find-matching-keys [db query]
  (filter (fn [k] (key-matches? query k) ) (keys db)))

(defn v-update
  [current update]
  (loop [acc {}
         current current
         update update]
    (if (empty? update)
      [acc current]
      (let [ [i n] (first update)
             o (get current i 0)]
        (if (> n o)
          (recur (assoc acc i n)
                 (assoc current i n)
                 (rest update)))))))

(defn db-update
  [db k v]
  (if (or (empty? v) (= (first k) \%))
    {}
    (let [[updates current] (v-update (get db k {}) v)]
      (def db (assoc db k current))
      updates)))

(defn db-search
  [db query]
  (let [ks (find-matching-keys db query)
        results (select-keys db ks)]
    results))

(defn db-lookup [db k] (get db k {}))

(defn find-neighbors
  [db]
  (distinct
    (map
      (keys (db-search db "n:%"))
      (fn [k] (take 2 (split (drop 2 k) #":"))))))

(defn pick-neighbor
  [db]
  (rand-nth (find-neighbors db)))

(defn store-neighbor
  [db host]
  (db-update db (str "n:" host) { 0 (/ (System/currentTimeMillis) 60) }))

(defn udp-msg
  [host port msg]
  {:host host :port port :message msg})

(defn is-search? [k] (re-matches #".*%.*" k))
(defn is-aggregate? [k] (re-matches #".*\*.*" k))

(defn message-type
  [{ k "key" v "vector" ttl "ttl"}]
  (cond
    (is-aggregate? k) :aggregate
    (is-search? k) :search
    :else :store))

(defmulti handle-message (fn [msg _ _ _] (message-type msg)))

(defmethod handle-message :store
  [{ k "key" v "vector" ttl "ttl"} ch _ _]
  (let [updates (db-update db k v)]
    (if (not-empty (:vector updates))
      (let [[host port] (pick-neighbor)
            to-send (udp-msg host port {:key k :vector updates :ttl (- ttl 1)})]
        (enqueue ch (mp/pack to-send))))))

(defmethod handle-message :aggregate
  [{ k "key" v "vector" ttl "ttl"} ch _ _] nil)

(defmethod handle-message :search
  [{ query "key" v "vector" ttl "ttl"} ch host port]
  (let [results (db-search db query)]
    (map results (fn [k v]
       (enqueue ch
          (udp-msg host port {:key k :vector v :ttl 50}))))))

(defn make-handle-datagram [ch]
  (fn [{ host :host port :port message :message }]
    (let [barray (.array message)
          msg (first (mp/unpack barray))]
      (println (str "received " msg " from " host))
      (store-neighbor db host)
      (handle-message msg ch host port))))

(defn run-server [port]
  (let [ch (deref (udp-socket {:port port}))]
    (receive-all ch (make-handle-datagram ch))))

(defn parse-int [s]
  (Integer/parseInt (re-find #"\A-?\d+" s)))

(defn -main [port]
  (println "Starting vignette node on port: " port)
  (run-server (parse-int port))
  (let [id (rand-int 10)]
    (loop [i 0]
      ;; (db-update db "count" { id i})
      (println "hll " (db-lookup db "hll"))
      (Thread/sleep 1000)
      (recur (+ i 1)))))

