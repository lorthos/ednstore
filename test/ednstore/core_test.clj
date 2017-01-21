(ns ednstore.core-test
  (:require [clojure.test :refer :all]
            [ednstore.common :refer :all]
            [ednstore.core :refer :all]
            [ednstore.store.segment :as s]
            [ednstore.store.reader :as r]
            [ednstore.env :as e]
            [ednstore.store.metadata :as md]
            [ednstore.store.merge.controller :as mcon])
  (:import (ednstore.core SimpleDiskStore)
           (java.util.concurrent Executors)))

(def test-db "core-test1")

(def S (atom nil))

(defn core-fixture [f]
  (let [exec-pool (Executors/newSingleThreadExecutor)
        merge-pool (mcon/make-merger-pool! 5)]
    (reset! S (SimpleDiskStore. exec-pool merge-pool))
    )
  (initialize! @S e/props)
  (f)
  (stop! @S))

(use-fixtures :each core-fixture)


(deftest simple-functionality-test
  (testing "with different types"
    (insert! @S test-db "A" "B")
    (is (= "B" (lookup @S test-db "A")))
    (insert! @S test-db :a :b)
    (is (= :b (lookup @S test-db :a)))
    (insert! @S test-db :a {:b "c"})
    (is (= {:b "c"} (lookup @S test-db :a)))
    (is (= "B" (lookup @S test-db "A")))
    (delete! @S test-db "A")
    (is (nil? (lookup @S test-db "A")))
    )
  )


(deftest init-test
  (testing "initialize when data already exists"
    ;build some state
    ;and init
    (insert! @S test-db "a0" "b0")
    (insert! @S test-db "a00" "b00")
    (insert! @S test-db "a000" "b000")
    (insert! @S test-db "a0000" "b0000")
    (s/roll-new-segment! test-db 1)
    (insert! @S test-db "a1" "b1")
    (insert! @S test-db "a11" "b11")
    (insert! @S test-db "a111" "b111")
    (s/roll-new-segment! test-db 2)
    (insert! @S test-db "a2" "b2")
    (insert! @S test-db "a22" "b22")
    (insert! @S test-db "a222" "b222")
    (stop! @S)

    (initialize! @S e/props)
    ;test previous segments
    (is (= "b0" (lookup @S test-db "a0")))
    (is (= "b00" (lookup @S test-db "a00")))
    (is (= "b000" (lookup @S test-db "a000")))
    (is (= "b1" (lookup @S test-db "a1")))
    (is (= "b11" (lookup @S test-db "a11")))
    (is (= "b111" (lookup @S test-db "a111")))
    ;test the active segment
    (is (= "b2" (lookup @S test-db "a2")))
    (is (= "b22" (lookup @S test-db "a22")))
    (is (nil? (r/read-direct "a2" (md/get-active-segment-for-table test-db))))

    )
  )
