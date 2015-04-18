(ns cljtable.store.reader-test
  (:require [clojure.test :refer :all]
            [cljtable.store.reader :refer :all]
            [cljtable.store.segment :as seg]
            [cljtable.store.writer :as wrt]))


(def segment (atom nil))

(defn segment-fixture [f]
  (reset! segment (seg/make-active-segment! "test.tbl"))
  (f)
  (seg/close-active-segment! @segment))

(use-fixtures :each segment-fixture)

(deftest read-from-segment
  (testing "read key from segment file"
    (wrt/write! "A" "B" @segment)                           ;offset 10
    (wrt/write! "AAAA" "BBBB" @segment)                     ;offset 10+16
    (is (= "B" (read-direct "A" @segment)))
    (is (= "BBBB" (read-direct "AAAA" @segment))))
  (testing "read when key does not exist"
    (is (nil? (read-direct "NON_EXISTING_KEY" @segment)))))
