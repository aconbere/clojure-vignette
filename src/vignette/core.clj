(ns vignette.core
  (:use [lamina core] [aleph udp] [clj-msgpack.core :as mp] [gloss core io]))

(def db {})

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

(defn db-query
  [db query]
  {})

(defn db-lookup [db k] (get db k {}))

(defn find-neighbors
  [db]
  (distinct (map (keys (db-query db "n:%")) #( (drop 2 %) ))))

(defn pick-neighbor
  [db]
  (rand-nth (find-neighbors db)))

(defn store-neighbor
  [db host]
  (db-update db (str "n:" host) { 0 (/ (System/currentTimeMillis) 60) }))

(defn udp-msg
  [host port msg]
  {:host host :port port :message msg})

(defn is-search [k] (re-matches #".*%.*" k))
(defn is-aggregate [k] (re-matches #".*\*.*" k))

(defn message-type
  [{ k "key" v "vector" ttl "ttl"}]
  (cond
    (is-aggregate k) :aggregate
    (is-search k) :search
    :else :store))

(defn make-key-regex [k]
  (re-pattern (clojure.string/replace k #"%" ".*")))

(defn key-matches? [k b]
  (boolean (re-matches (make-key-regex k) b)))

(defn find-matching-keys [db query]
  (filter (fn [k] (key-matches? query k) ) (keys db)))

(defmulti handle-message (fn [msg _ _ _] (message-type msg)))

(defmethod handle-message :store
  [{ k "key" v "vector" ttl "ttl"} ch host port]
  (let [updates (db-update db k v)]
    (if (not-empty (:vector updates))
      (let [to-send (udp-msg
                     (pick-neighbor)
                     port
                     {:key k :vector updates :ttl (- ttl 1)})]
        (enqueue ch (mp/pack to-send))))))

(defmethod handle-message :aggregate [{ k "key" v "vector" ttl "ttl"} ch host port] nil)

(defmethod handle-message :search
  [{ query "key" v "vector" ttl "ttl"} ch host port]
  (let [ks (find-matching-keys db query)
        response (select-keys db ks)]
    (map response
         (fn [k v]
           (enqueue ch
                    (udp-msg host port {:key k :vector v :ttl 50}))))))

(defn run-server [port]
  (let [ch (deref (udp-socket {:port port}))]
    (receive-all
      ch
      (fn [{ host :host _port :port message :message }]
        (let [barray (.array message)
              msg (first (mp/unpack barray))]
          (println (str "received " msg " from " host))
          (store-neighbor db host)
          (handle-message msg ch host port))))))
