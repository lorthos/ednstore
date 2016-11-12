(ns ednstore.store.segment-test
  (:require [clojure.test :refer :all]
            [ednstore.store.segment :refer :all]
            [ednstore.store.writer :as w]
            [ednstore.store.reader :as r]
            [ednstore.store.metadata :as md]))

(def test-db-ns "segment-test1")

(defn segment-fixture [f]
  (reset! md/store-meta {})
  (f)
  (close-segment! (md/get-active-segment-for-namespace test-db-ns)))

(use-fixtures :each segment-fixture)

(deftest roll-new-segment-test
  (testing "adding a new segment and querying afterwards"
    (is (nil? (md/get-active-segment-for-namespace test-db-ns)))
    (is ((comp not nil?) (do
                           (roll-new-segment! test-db-ns 0)
                           (md/get-active-segment-for-namespace test-db-ns))))
    (w/write! test-db-ns "A" "B")
    (is (= "B" (r/read-direct "A" (md/get-active-segment-for-namespace test-db-ns))))
    (roll-new-segment! test-db-ns 1)
    (is (nil? (r/read-direct "A" (md/get-active-segment-for-namespace test-db-ns))))
    (w/write! test-db-ns "AA" "BB" )
    (is (= "BB" (r/read-direct "AA" (md/get-active-segment-for-namespace test-db-ns))))
    ))



