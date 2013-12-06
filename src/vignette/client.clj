(ns vignette.client
  (:require [clojure.core.async :refer [>! go]]
            [vignette.core :as core]
            [vignette.hll :as hll]))

(defn client-send
  [server k v]
  (go (>! (:in server) (core/datagram server {:k k :v v}))))

(defn query
  [server k]
  (client-send server k {}))

(defn get-v
  [server k]
  ((deref (:db server)) k))
