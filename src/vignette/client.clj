(ns vignette.client
  (:require [clojure.core.async :refer [>! >!! go]]
            [vignette.core :as core]
            [vignette.hll :as hll]))

(defn -send
  [server cmd]
   (>!! (:cmd server) cmd))

(defn query [server k] (-send server { :type :query :key k }))
(defn store [server k v] (-send server { :type :store :key k :vector v}))

(defn getv [server k] (get (deref (:db server)) k {}))
(defn add-neighbor
  [server neighbor]
  (store server (str "n:" (core/host->string neighbor)) {0 (System/currentTimeMillis)}))
