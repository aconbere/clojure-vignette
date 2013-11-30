(ns vignette.core
  (:use [aleph udp]))

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
  (distinct (map (keys (db-query db "n:%")) #{ (drop 2 %) })))

(defn pick-neighbor
  [db]
  (rand-nth (find-neighbors db)))

(defn store-neighbor
  [db host]
  (db-update db (str "n:" host) { 0 (/ (System/currentTimeMillis) 60) }))

(defn udp-msg
  [host port msg]
  {:host host :port port :message msg})

(defn message-type
  [{ k :key v :vector ttl :ttl}]
  (cond
    (is-aggregate k) :aggregate
    (is-search k) :search
    :store))

(defmulti handle-msg message-type)

(defmethod :store [{ k :key v :vector ttl :ttl} ch host port]
  (let [updates (db-update db k v))
    (if (not-empty (:vector updates))
      (let [to-send (udp-msg
                     (pick-neighbor)
                     port
                     {:key k :vector updates :ttl (- ttl 1)})]
        (enqueue ch (mp/pack-into to-send)))))

(defmethod :aggregate [{ k :key v :vector ttl :ttl} ch host port] nil)
(defmethod :search [{ k :key v :vector ttl :ttl} ch host port] nil)

(defn run-server [port]
  (let [ch (udp-socket {:port (int port)})]
    (receive-all
      (fn [{ host :host _port :port message :message }]
        (handle-msg (mp/unpack message) ch host port)))))
