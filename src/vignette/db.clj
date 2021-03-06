(ns vignette.db)

(defn- make-key-regex [k]
  (re-pattern (clojure.string/replace k #"%" ".*")))

(defn- key-matches? [k b]
  (boolean (re-matches (make-key-regex k) b)))

(defn- find-matching-keys [db query]
  (filter (fn [k] (key-matches? query k) ) (keys db)))

(defn vector-update
  "takes two sparse vectors represented as maps from integer indexes
  to indexes and applies an element wise max to them."
  [current update]
  (loop [acc {}
         current current
         update update]
    (if (empty? update)
      [acc current]
      (let [[i n] (first update)
            o (get current i 0)]
        (if (> n o)
          (recur (assoc acc i n)
                 (assoc current i n)
                 (rest update))
          (recur acc current (rest update)))))))

(defn lookup
  [db k]
  (get db k {}))

(defn update
  [db k v]
  (if (empty? v)
    [db (lookup db k)]
    (let [[updates current] (vector-update (get db k {}) v)
          db (assoc db k current)]
      [db updates])))

(defn search
  [db query]
  (let [ks (find-matching-keys db query)
        results (select-keys db ks)]
    results))

