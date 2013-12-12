(ns vignette.core
  (:require [ac.udp :refer [udp-socket]]
            [clojure.core.async :refer [<! >! go go-loop timeout chan]]
            [clj-msgpack.core :as mp :refer [pack unpack]]
            [clojure.string :refer [split join]]
            [vignette.db :as vdb]
            [vignette.hll :as hll]
            [clojure.set :refer [difference]]
            [clojure.math.numeric-tower :refer [floor]]
            ))

(defn parse-int [s]
  (Integer/parseInt (re-find #"\A-?\d+" s)))

(defn datagram->message
  "Get and parse a message off of a datagram"
  [{ host :host port :port message :message }]
  (let [barray (.array message)]
    (first (unpack barray))))

(defn datagram->host
  "grab the hosty bits off a datagram"
  [{ host :host port :port message :message }]
  { :host host :port port})

(defn string->host
  [host-string]
  (let [[host port] (take 2 (split host-string #":"))]
    { :host host :port (parse-int port)}))

(defn host->string
  [{host :host port :port}]
  (join ":" [host port]))

(defn datagram
  [host msg]
  {:host (:host host) :port (:port host) :message (mp/pack msg)})

(defn key->host
  [neighbor-key]
  (string->host (subs neighbor-key 2)))

(defn find-neighbors
  "Returns a map from hosts of the type { :host <host> :port <port> } to a timestamp"
  [db]
  (reduce
    (fn [acc [k v]] (assoc acc (key->host k) (get v 0)))
    {}
    (vdb/search db "n:%")
    ))

(defn pick-neighbors
  "returns a list of hosts of the type { :host <host> :port <port> }"
  [db n filtered-hosts]
  (let [neighbors (find-neighbors db)
        hosts (difference (set (keys neighbors)) filtered-hosts)
        hosts (select-keys neighbors hosts)]
    (shuffle (take n (map first (seq hosts))))))

(defn store-neighbor
  [db host]
  (let [t (System/currentTimeMillis)
        k (str "n:" (host->string host))
        v { 0 t }
        db (first (vdb/update db k v))]
    db))

(defn query-neighbors
  [out neighbors msg]
  (doseq [to neighbors]
    (println "sending" msg "to" to)
    (go (>! out (datagram to msg)))))

(defn connection
  [host] (udp-socket {:port (:port host)}))

(defn heartbeat
  [server n]
  (let [k (str "n:" (host->string (:host server)))
        v {0 (System/currentTimeMillis)}
        msg {"key" k "vector" v "full" true}]
    (query-neighbors
      (:udp-out server)
      (pick-neighbors (deref (:db server)) n #{(:host server)})
      msg)))

(def default-opts {:timeout 3000 :heartbeat true})

(defn vignette
  ([port neighbors]
   (vignette port neighbors default-opts))
  ([port neighbors opts]
    (let [host {:host "127.0.0.1" :port port}
          db (agent (reduce store-neighbor {} neighbors))
          [in out] (connection host)
          cmd (chan)
          server {:db db
                  :host host
                  :in in
                  :out (chan)
                  :udp-out out
                  :cmd cmd
                  :opts opts}]
      server)))

(defn is-search? [k] (re-matches #".*%.*" k))
(defn is-aggregate? [k] (re-matches #".*\*.*" k))
(defn message-type
  [{ k "key" v "vector" ttl "ttl"}]
  (cond
    (is-aggregate? k) :aggregate
    (is-search? k) :search
    :else :store))

(defn full-message?
  [msg]
  (boolean (get msg "full" false)))

;; returns things like
;; [[<key> <bool>] <vector>]
;;
;; The bool is to namespace partial and full vectors
(defn compress-inner
  [acc msg]
  (let [k [(msg "key") (full-message? msg)]
        v (msg "vector")
        current (get acc k {})
        n (second (vdb/vector-update current v))]
    (assoc acc k n)))

(defn compress-messages
  [msgs]
  (let [compressed (reduce compress-inner {} msgs)]
    (map (fn [[[k full?] v]] (if full?  {"key" k "vector" v "full" true} {"key" k "vector" v}))
         compressed)))
