(ns vignette.examples
  (:require [clojure.core.async :as async :refer [<! >! go go-loop timeout]]
            [vignette.db :as vdb]
            [vignette.core :as core]
            [vignette.hll :as hll]
            [vignette.client :as client]
            [vignette.server :as server]
            ))

(defn handle-hll
  [datagram]
  (let [{ v "vector" } (core/datagram->message datagram)
       est (hll/estimate v)]
    (println est)))

(defn example-server
  [port]
  (let [neighbors ["127.0.0.1:6000"]
        port port
        opts { :heartbeat false }
        server (server/run (core/vignette port (map #(core/string->host %) neighbors) opts))]
    server))

(defn example-run
  [server]
  (client/store server "hll" (hll/vectorize (str (rand)))))
