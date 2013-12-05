(ns vignette.hll-test
  (:require [clojure.test :refer :all]
            [vignette.hll :refer :all]))

(deftest test-update
  (testing "Test hll vectorize"
    (vectorize (rand))))

