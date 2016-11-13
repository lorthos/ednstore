(ns ednstore.load-test
  (:require [clojure.test :refer :all]
            [ednstore.common :refer :all]
            [ednstore.core :refer :all]
            [ednstore.env :as e])
  (:import (ednstore.core SimpleDiskStore)))

(def test-db "load-test1")

(def S (atom nil))

(defn segment-fixture [f]
  (reset! S (SimpleDiskStore.))
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

