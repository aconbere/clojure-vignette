(ns vignette.client
  (:require [ac.udp :as udp :refer [udp-socket]]
            [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [clj-msgpack.core :as mp :refer [pack unpack]]
            [vignette.db :as vdb]
            [clojure.math.numeric-tower :as math :refer [floor]]
            [clojure.string :as string :refer [split]]
            ))

(defn search
  [ch query]
  (go (>! ch (udp-msg (mp/pack)))))
