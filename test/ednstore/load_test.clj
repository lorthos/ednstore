(ns ednstore.load-test
  (:require [clojure.test :refer :all]
            [ednstore.common :refer :all]
            [ednstore.core :refer :all]
            [ednstore.env :as e])
  (:import (ednstore.core SimpleDiskStore)))

(def S (atom nil))

(defn segment-fixture [f]
  (reset! S (SimpleDiskStore.))
  (initialize! @S e/props)
  (f)
  (stop! @S))

(use-fixtures :each segment-fixture)


(deftest load-test
  (testing "load-100k-items"
    (doseq [x (range 10000)]
      (let [v (str x (java.util.UUID/randomUUID))]
        (Thread/sleep 100)
        (insert! @S x v)))
    (is (string? (lookup @S 42)))))

