(ns vignette.server
  (:require [clojure.core.async :as async :refer [<! >! go go-loop timeout]]
            [vignette.db :as vdb]
            [vignette.core :as core]
            ))

(defn update-db-agent
  [state k v]
  (first (vdb/update state k v)))

(defmulti handle-message
  (fn [server from msg]
    (core/message-type msg)))

(defmethod handle-message :store
  [server from msg]
  (let [{k "key" v "vector"} msg
        state (deref (:db server))
        [state updates] (vdb/update state k v)]
    (when (not (empty? updates))
      ;; if we have never seen this key and this is a partial message
      ;; state of the key, go query the network for the full state
      (when (and (not (contains? state k)) (not (core/full-message? msg)))
        (go (>! (:out server) {"key" k "vector" (vdb/lookup state k) "full" true}))) 
      (go (>! (:out server) {"key" k "vector" updates}))
      (send (:db server) update-db-agent k updates))))

(defmethod handle-message :aggregate
  [server from msg]
  nil)

(defmethod handle-message :search
  [server from {query "key" v "vector"}]
  (let [state (deref (:db server))
        results (vdb/search state query)]
    (doseq [k (keys results)]
      (go (>! :out server) {"key" k "vector" (results k)}))))

(defmulti handle-command (fn [server cmd] (cmd "type")))

(defmethod handle-command :store
  [server cmd]
  (handle-message server (:host server) cmd))

(defmethod handle-command :query
  [server cmd]
  (core/query-neighbors
    (:udp-out server)
    (core/pick-neighbors (deref (:db server)) 3 #{(:host server)})
    {(cmd "key") {} "full" true }))

(defn command-loop
  [server]
  (go-loop [cmd (<! (:cmd server))]
    (println "received cmd" cmd)
    (handle-command server cmd)
    (recur (<! (:cmd server)))))

(defn message-loop
  [server]
  (go-loop [datagram (<! (:in server))]
    (let [opts (:opts server)
          msg (core/datagram->message datagram)
          incoming-host (core/datagram->host datagram)]
      (println "received msg" msg "from" incoming-host)
      (handle-message server incoming-host msg))
    (recur (<! (:in server)))))

(defn -do-output
  [server msgs]
  (doseq [msg (core/compress-messages msgs)]
    (core/query-neighbors
      (:udp-out server)
      (core/pick-neighbors (deref (:db server)) 4 #{(:host server)})
      msg)))

(defn output-loop
  [server]
  (if (-> server :opts :timeout)
    (go-loop [msgs (list (<! (:out server)))]
      (<! (timeout (-> server :opts :timeout)))
      (-do-output server msgs)
      (core/heartbeat server 4)
      (recur (conj msgs (<! (:out server)))))
    (go-loop [msg (<! (:out server))]
      (-do-output server [msg])
      (recur (<! (:out server))))))

(defn run
  [server]
  (message-loop server)
  (command-loop server)
  (output-loop server)
  server)

(defn -main [port & neighbors]
  (println "Starting vignette node on port:" port "with neighbors: " neighbors)
  (run
    (core/vignette
      (core/parse-int port)
      (map #(core/string->host %) neighbors)
      )))
