(ns ednstore.store.segment-test
  (:require [clojure.test :refer :all]
            [ednstore.store.segment :refer :all]
            [ednstore.store.writer :as w]
            [ednstore.store.reader :as r]))

(defn segment-fixture [f]
  (reset! active-segment nil)
  (reset! old-segments {})
  (f)
  (close-segment! @active-segment)
  (reset! active-segment nil))

(use-fixtures :each segment-fixture)

(deftest roll-new-segment-test
  (testing "adding a new segment and querying afterwards"
    (is (nil? @active-segment))
    (is ((comp not nil?) (do
                           (roll-new-segment! 0)
                           @active-segment)))
    (w/write! "A" "B" @active-segment)
    (is (= "B" (r/read-direct "A" @active-segment)))
    (roll-new-segment! 1)
    (is (nil? (r/read-direct "A" @active-segment)))
    (w/write! "AA" "BB" @active-segment)
    (is (= "BB" (r/read-direct "AA" @active-segment)))
    ))



