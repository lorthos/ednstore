(ns cljtable.core-test
  (:require [clojure.test :refer :all]
            [cljtable.core :refer :all]
            [cljtable.store.segment :as s]))


(defn segment-fixture [f]
  (s/roll-new-segment! 0)
  (f)
  (s/close-active-segment! @s/active-segment))

(use-fixtures :each segment-fixture)


(deftest simple-functionality-test
  (testing "with different types"
    (insert! "A" "B")
    (is (= "B" (lookup "A")))
    (insert! :a :b)
    (is (= :b (lookup :a)))
    (insert! :a {:b "c"})
    (is (= {:b "c"} (lookup :a)))
    )
  )
