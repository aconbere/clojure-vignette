(ns vignette.hll
  (:require [pandect.core :refer [md5-bytes]]
            [clojure.math.numeric-tower :refer [floor round expt]]
            ))

(def default-opts { :buckets 1024 :a 0.721 :two-32 (expt 2 32) })

(defn vectorize 
  ([in] (vectorize in {}))
  ([in opts]
    (let [opts (merge opts default-opts)
          buckets (:buckets opts)
          h (BigInteger. (md5-bytes (str in)))
          bucket (.and h (BigInteger. (str (- buckets 1))))
          counter (- 32 (floor (/ (Math/log h) (Math/log 2))))]
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
