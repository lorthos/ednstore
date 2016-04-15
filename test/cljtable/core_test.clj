(ns cljtable.core-test
  (:require [clojure.test :refer :all]
            [cljtable.common :refer :all]
            [cljtable.core :refer :all]
            [cljtable.store.segment :as s]
            [cljtable.store.reader :as r]
            [cljtable.env :as e])
  (:import (cljtable.core SimpleDiskStore)))

(def S (atom nil))

(defn segment-fixture [f]
  (reset! S (SimpleDiskStore.))
  (initialize! @S e/props)
  (f)
  (stop! @S)
  (reset! s/active-segment nil))

(use-fixtures :each segment-fixture)


(deftest simple-functionality-test
  (testing "with different types"
    (insert! @S "A" "B")
    (is (= "B" (lookup @S "A")))
    (insert! @S :a :b)
    (is (= :b (lookup @S :a)))
    (insert! @S :a {:b "c"})
    (is (= {:b "c"} (lookup @S :a)))
    (is (= "B" (lookup @S "A")))
    (delete! @S "A")
    (is (nil? (lookup @S "A")))
    )
  )


(deftest init-test
  (testing "initialize when data already exists"
    ;build some state
    ;and init
    (insert! @S "a0" "b0")
    (insert! @S "a00" "b00")
    (insert! @S "a000" "b000")
    (insert! @S "a0000" "b0000")
    (s/roll-new-segment! 1)
    (insert! @S "a1" "b1")
    (insert! @S "a11" "b11")
    (insert! @S "a111" "b111")
    (s/roll-new-segment! 2)
    (insert! @S "a2" "b2")
    (insert! @S "a22" "b22")
    (insert! @S "a222" "b222")
    ;(s/roll-new-segment! 3)
    (stop! @S)
    (initialize! @S e/props)
    ;test previous segments
    (is (= "b0" (lookup @S "a0")))
    (is (= "b00" (lookup @S "a00")))
    (is (= "b000" (lookup @S "a000")))
    (is (= "b1" (lookup @S "a1")))
    (is (= "b11" (lookup @S "a11")))
    (is (= "b111" (lookup @S "a111")))
    ;test the active segment
    (is (= "b2" (lookup @S "a2")))
    (is (= "b22" (lookup @S "a22")))
    (is (nil? (r/read-direct "a2" @s/active-segment)))

    )
  )
