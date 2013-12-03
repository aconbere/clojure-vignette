(ns vignette.core-test
  (:require [clojure.test :refer :all]
            [vignette.core :refer :all]))

(deftest test-find-neighbors
  (testing "Find Neighbor"
    (let [db (store-neighbor {} "127.0.0.1" 7777)
          db (store-neighbor db "127.0.0.1" 6666)
          expected '(("127.0.0.1" 7777) ("127.0.0.1" 6666))]
      (is (= expected (find-neighbors db))))))

(deftest test-pick-neighbor
  (testing "Pick Neighbor"
    (let [db (store-neighbor {} "127.0.0.1" 7777)
          db (store-neighbor db "127.0.0.1" 6666)]
      (is (= '("127.0.0.1" 7777) (pick-neighbor db "127.0.0.1" 6666))))))
