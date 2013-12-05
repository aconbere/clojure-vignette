(ns vignette.client
  (:require [ac.udp :refer [udp-socket]]
            [clojure.core.async :refer [<! >! go go-loop]]
            [clj-msgpack.core :as mp :refer [pack unpack]]
            [clojure.string :refer [split join]]
            [vignette.hll :as hll]
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

(defn udp-msg
  [{ host :host port :port} msg]
  {:host host :port port :message (mp/pack msg)})

(defn make-client [port] (udp-socket {:port port}))

(defn send-updates
  [neighbors out k updates ttl]
  (doseq [neighbor neighbors]
    (let [m {"key" k "vector" updates "ttl" ttl}
          msg (udp-msg neighbor m)]
      (go (>! out msg)))))

(defn query-neighbors
  [neighbors out k]
  (send-updates neighbors out k {} 10))

(defn listen
  [[in out] f]
  (go-loop
    [datagram (<! in)]
    (f datagram))
  [in out])

(defn handle-hll
  [datagram]
  (let [{ v "vector" } (datagram->message datagram)
       est (hll/estimate v)]
    (println est)))

