(ns ednstore.core-test
  (:require [clojure.test :refer :all]
            [ednstore.common :refer :all]
            [ednstore.core :refer :all]
            [ednstore.store.segment :as s]
            [ednstore.store.reader :as r]
            [ednstore.env :as e]
            [ednstore.store.metadata :as md])
  (:import (ednstore.core SimpleDiskStore)))

(def test-db-ns "core-test1")

(def S (atom nil))

(defn core-fixture [f]
  (reset! S (SimpleDiskStore.))
  (initialize! @S e/props)
  (f)
  (stop! @S))

(use-fixtures :each core-fixture)


;(deftest simple-functionality-test
;  (testing "with different types"
;    (insert! @S test-db-ns "A" "B")
;    (is (= "B" (lookup @S test-db-ns "A")))
;    (insert! @S test-db-ns :a :b)
;    (is (= :b (lookup @S test-db-ns :a)))
;    (insert! @S test-db-ns :a {:b "c"})
;    (is (= {:b "c"} (lookup @S test-db-ns :a)))
;    (is (= "B" (lookup @S test-db-ns "A")))
;    (delete! @S test-db-ns "A")
;    (is (nil? (lookup @S test-db-ns "A")))
;    )
;  )
;

(deftest init-test
  (testing "initialize when data already exists"
    ;build some state
    ;and init
    (insert! @S test-db-ns "a0" "b0")
    (insert! @S test-db-ns "a00" "b00")
    (insert! @S test-db-ns "a000" "b000")
    (insert! @S test-db-ns "a0000" "b0000")
    (s/roll-new-segment! test-db-ns 1)
    (insert! @S test-db-ns "a1" "b1")
    (insert! @S test-db-ns "a11" "b11")
    (insert! @S test-db-ns "a111" "b111")
    (s/roll-new-segment! test-db-ns 2)
    (insert! @S test-db-ns "a2" "b2")
    (insert! @S test-db-ns "a22" "b22")
    (insert! @S test-db-ns "a222" "b222")
    (stop! @S)

    (initialize! @S e/props)
    ;test previous segments
    (is (= "b0" (lookup @S test-db-ns "a0")))
    (is (= "b00" (lookup @S test-db-ns "a00")))
    (is (= "b000" (lookup @S test-db-ns "a000")))
    (is (= "b1" (lookup @S test-db-ns "a1")))
    (is (= "b11" (lookup @S test-db-ns "a11")))
    (is (= "b111" (lookup @S test-db-ns "a111")))
    ;test the active segment
    (is (= "b2" (lookup @S test-db-ns "a2")))
    (is (= "b22" (lookup @S test-db-ns "a22")))
    (is (nil? (r/read-direct "a2" (md/get-active-segment-for-namespace test-db-ns))))

    )
  )
