(ns vignette.hll
  (:require [clojure.math.numeric-tower :refer [floor round expt abs]])
  (:import (com.google.common.hash Hashing)))

(def default-opts { :buckets 1024 :a 0.721 :two-32 (expt 2 32) })

(defn hash-string
  [string]
  (-> (Hashing/murmur3_32) (.hashString string) .asInt))

(defn vectorize 
  ([in] (vectorize in {}))
  ([in opts]
    (let [opts (merge opts default-opts)
          buckets (:buckets opts)
          h (hash-string (str in))
          bucket (bit-and h (- buckets 1))
          counter (- 32 (floor (/ (Math/log (abs h)) (Math/log 2))))]
      { bucket counter })))

(defn estimate
  ([v] (estimate v {}))
  ([v opts]
    (let [opts (merge opts default-opts)
          a (:a opts)
          buckets (:buckets opts)
          sum (reduce + (map #(* -2.0 %) (vals v)))
          zeros (- buckets (count v))
          est (/ (* a buckets buckets) (+ sum zeros))
          t (:two-32 opts)]
      (round
        (cond
          (and (< est 2500) (> zeros 0)) (* buckets (Math/log (/ buckets zeros)))
          (> est (/ t 30)) (* (- t (Math/log (- 1 (/ est t))))) 
          :else est)))))
