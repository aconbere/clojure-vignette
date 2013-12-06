(ns vignette.client
  (:require [ac.udp :refer [udp-socket]]
            [clojure.core.async :refer [<! >! go go-loop]]
            [clj-msgpack.core :as mp :refer [pack unpack]]
            [clojure.string :refer [split join]]
            [vignette.hll :as hll]
            ))

(comment
(defn handle-hll
  [datagram]
  (println "handler")
  (let [{ v "vector" } (datagram->message datagram)
       est (hll/estimate v)]
    (println est))))
