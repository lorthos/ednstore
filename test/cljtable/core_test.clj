(ns cljtable.core-test
  (:require [clojure.test :refer :all]
            [cljtable.core :refer :all]
            [cljtable.store.segment :as s]
            [cljtable.store.reader :as r]))


(defn segment-fixture [f]
  (s/roll-new-segment! 0)
  (f)
  (s/close-segment-fully! @s/active-segment)
  (reset! s/active-segment nil))

(use-fixtures :each segment-fixture)


(deftest simple-functionality-test
  (testing "with different types"
    (insert! "A" "B")
    (is (= "B" (lookup "A")))
    (insert! :a :b)
    (is (= :b (lookup :a)))
    (insert! :a {:b "c"})
    (is (= {:b "c"} (lookup :a)))
    (is (= "B" (lookup "A")))
    (delete! "A")
    (is (nil? (lookup "A")))
    )
  )


(deftest init-test
  (testing "initialize when data already exists"
    ;build some state
    ;and init
    (insert! "a0" "b0")
    (insert! "a00" "b00")
    (insert! "a000" "b000")
    (insert! "a0000" "b0000")
    (s/roll-new-segment! 1)
    (insert! "a1" "b1")
    (insert! "a11" "b11")
    (insert! "a111" "b111")
    (s/roll-new-segment! 2)
    (insert! "a2" "b2")
    (insert! "a22" "b22")
    (insert! "a222" "b222")
    ;(s/roll-new-segment! 3)
    (stop!)
    (println "STOPPED")
    (initialize!)
    ;test previous segments
    (is (= "b0" (lookup "a0")))
    (is (= "b00" (lookup "a00")))
    (is (= "b000" (lookup "a000")))
    (is (= "b1" (lookup "a1")))
    (is (= "b11" (lookup "a11")))
    (is (= "b111" (lookup "a111")))
    ;test the active segment
    (is (= "b2" (lookup "a2")))
    (is (= "b22" (lookup "a22")))
    (is (nil? (r/read-direct "a2" @s/active-segment)))
    (println "ACTIVE" @s/active-segment)
    (println "OLD" @s/old-segments)

    )
  )
