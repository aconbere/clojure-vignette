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
    (let [[updates current] (v-update (get db k {}))]
      (def db (assoc db k current))
      updates)))

(defn db-query
  [db query]
  {})

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

(defn is-aggregate [k] false)
(defn is-search [k] false)

(defn message-type
  [{ k "key" v "vector" ttl "ttl"}]
  (cond
    (is-aggregate k) :aggregate
    (is-search k) :search
    :else :store))

(defmulti handle-msg (fn [msg _ _ _] (message-type msg)))

(defmethod handle-msg :store [{ k "key" v "vector" ttl "ttl"} ch host port]
  (let [updates (db-update db k v)]
    (if (not-empty (:vector updates))
      (let [to-send (udp-msg
                     (pick-neighbor)
                     port
                     {:key k :vector updates :ttl (- ttl 1)})]
        (enqueue ch (mp/pack to-send))))))

(defmethod handle-msg :aggregate [{ k "key" v "vector" ttl "ttl"} ch host port] nil)
(defmethod handle-msg :search [{ k "key" v "vector" ttl "ttl"} ch host port] nil)

(defn run-server [port]
  (let [ch (deref (udp-socket {:port port}))]
    (receive-all
      ch
      (fn [{ host :host _port :port message :message }]
        (let [barray (.array message)
              msg (first (mp/unpack barray))]
          (println (str "received " msg " from " host))
          (handle-msg msg ch host port))))))
