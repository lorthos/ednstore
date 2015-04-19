(ns cljtable.core-test
  (:require [clojure.test :refer :all]
            [cljtable.store.reader :as rdr]
            [cljtable.store.segment :as seg]
            [cljtable.store.writer :as wrt]))


(def segment (atom nil))

(defn segment-fixture [f]
  (reset! segment (seg/make-active-segment! 33 "33.tbl"))
  (f)
  (seg/close-active-segment! @segment))

(use-fixtures :each segment-fixture)


(deftest simple-functionality-test
  (testing "with different types"
    (wrt/write! "A" "B" @segment)
    (is (= "B" (rdr/read-direct "A" @segment)))
    (wrt/write! :a :b @segment)
    (is (= :b (rdr/read-direct :a @segment)))
    (wrt/write! :a {:b "c"} @segment)
    (is (= {:b "c"} (rdr/read-direct :a @segment)))
    )
  )
