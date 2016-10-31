(ns cljtable.load-test
  (:require [clojure.test :refer :all]
            [cljtable.common :refer :all]
            [cljtable.core :refer :all]
            [cljtable.env :as e])
  (:import (cljtable.core SimpleDiskStore)))

(def S (atom nil))

(defn segment-fixture [f]
  (reset! S (SimpleDiskStore.))
  (initialize! @S e/props)
  (f)
  (stop! @S))

(use-fixtures :each segment-fixture)


(deftest load-test
  (testing "load-100k-items"
    (doseq [x (range 100000)]
      (let [v (str x (java.util.UUID/randomUUID))]
        (insert! @S x v)
        )
      )
    (is (string? (lookup @S 42)))
    )
  )

