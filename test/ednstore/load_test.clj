(ns ednstore.load-test
  (:require [clojure.test :refer :all]
            [ednstore.common :refer :all]
            [ednstore.core :refer :all]
            [ednstore.env :as e]
            [ednstore.store.merge.controller :as mcon])
  (:import (ednstore.core SimpleDiskStore)
           (java.util.concurrent Executors)))

(def test-db "load-test1")

(def S (atom nil))

(defn segment-fixture [f]
  (let [exec-pool (Executors/newSingleThreadExecutor)
        merge-pool (mcon/make-merger-pool! 5)]
    (reset! S (SimpleDiskStore. exec-pool merge-pool))
    )
  (initialize! @S e/props)
  (f)
  (stop! @S))

(use-fixtures :each segment-fixture)


(deftest load-test
  (testing "load-100k-items"
    (doseq [x (range 1000)]
      (let [v (str x (java.util.UUID/randomUUID))]
        (Thread/sleep 10)
        (insert! @S test-db x v)))
    (is (string? (lookup @S test-db 42)))))

