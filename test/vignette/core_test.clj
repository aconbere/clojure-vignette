(ns vignette.core-test
  (:require [clojure.test :refer :all]
            [vignette.core :refer :all]))

(defn h [host port]
  { :host host :port port})

(deftest test-pick-neighbors
  (testing "Pick Neighbor"
    (let [self (h "127.0.0.1" 6666)
          other (h "127.0.0.1" 7777)
          db (-> {} (store-neighbor other) (store-neighbor self))]
      (is (= (list other) (pick-neighbors db 1 #{ self }))))))

(deftest test-store-neighbor
  (testing "Store Neighbor"
    (is (true? (contains? (store-neighbor {} { :host "127.0.0.1" :port 6666 }) "n:127.0.0.1:6666")))))

(deftest test-compress-inner
  (testing "compress-inner"
    (is (= {["k" false] {0 5}} (compress-inner {} { "key" "k" "vector" {0 5}})))
    (is (= {["k" false] {0 5 1 6}} (compress-inner {["k" false] {1 6}} { "key" "k" "vector" {0 5} })))
    ))
