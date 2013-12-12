(ns vignette.db-test
  (:require [clojure.test :refer :all]
            [vignette.db :refer :all]))

(deftest test-update
  (testing "update"
    (let [db {}]
      (let [expected [{ "x" { 1 2 3 4 }} { 1 2 3 4 }]
            input (update db "x" { 1 2 3 4 } )]
        (is (= expected input ))))))

(deftest test-vector-update
  (testing "vector-update"
    (is (= [{1 6} {0 5 1 6}] (vector-update {0 5} {1 6})))))
