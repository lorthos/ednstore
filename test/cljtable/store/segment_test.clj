(ns cljtable.store.segment-test
  (:require [clojure.test :refer :all]
            [cljtable.store.segment :refer :all]))


(def root-path "target/segments/")

(deftest roll-new-segment-test
  (testing "adding a new segment"
    ;TODO
    (is (nil? @active-segment))
    (is (nil? (do
                (roll-new-segment! 0)
                @active-segment)))
    ))
