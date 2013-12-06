(ns vignette.server
  (:require [clojure.core.async :as async :refer [<! >! go go-loop timeout]]
            [vignette.db :as vdb]
            [vignette.core :as core]
            ))

(defn update-db-agent
  [state k v]
  (first (vdb/update state k v)))

(defmulti handle-message
  (fn [server from msg] (core/message-type msg)))

(defmethod handle-message :store
  [server from msg]
  (let [{k "key" v "vector"} msg
        state (deref (:db server))
        [state updates] (vdb/update state k v)
        filtered-hosts #{(:host server) from}
        neighbors (core/pick-neighbors state 3 filtered-hosts)]

    (when (not-empty updates)
      ;; if we have never seen this key and this is a partial message
      ;; state of the key, go query the network for the full state
      (when (and (not (contains? state k)) (not (core/full-message? msg)))
        (core/query-neighbors
          (:out server)
          (core/pick-neighbors state 3 #{(:host server)})
          (vdb/lookup state k)
          { "full" true }))
      (core/query-neighbors (:out server) neighbors k updates)
      (send (:db server) update-db-agent k updates))))

(defmethod handle-message :aggregate
  [server from msg]
  nil)

(defmethod handle-message :search
  [server from {query "key" v "vector"}]
  (let [state (deref (:db server))
        results (vdb/search state query)]
    (doseq [k (keys results)]
      (let [v (results k)]
        (core/do-send (:out server) from {"key" k "vector" v})))))

(defn run
  [server]
  (go-loop [datagram (<! (:in server))]
    (let [opts (:opts server)
          msg (core/datagram->message datagram)
          incoming-host (core/datagram->host datagram)]
      (println "received" msg "from" incoming-host)
      (handle-message server incoming-host msg))
    (recur (<! (:in server))))

  (when (-> server :opts :heartbeat) (core/heartbeat server))
  server)

(defn -main [port & neighbors]
  (println "Starting vignette node on port:" port "with neighbors: " neighbors)
  (run (core/vignette
         (core/parse-int port)
         (map #(core/string->host %) neighbors))))

