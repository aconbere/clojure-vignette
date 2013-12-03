(ns vignette.db-test
  (:require [clojure.test :refer :all]
            [vignette.db :refer :all]))

(deftest test-update
  (testing "Test db update"
    (let [db {}
          expected [{ "x" { 1 2 3 4 }} { 1 2 3 4 }]
          input (update db "x" { 1 2 3 4 } )]
      (is (= expected input )))))

