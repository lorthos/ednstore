(ns cljtable.store.merger-test
  (:require [clojure.test :refer :all]
            [cljtable.store.merger :refer :all]
            [cljtable.store.segment :as s]))


(deftest merge-candidates
  (testing "get merge candidates"
    (is (= '(1000 1001) (get-merge-candidate-ids '(1000 1001))))
    (is (= '(1000 1001) (get-merge-candidate-ids '(1000 1001 1002))))
    (is (= '(0 1002) (get-merge-candidate-ids '(0 1002))))
    (is (nil? (get-merge-candidate-ids '(0))))
    (is (nil? (get-merge-candidate-ids '(1002))))
    ))
