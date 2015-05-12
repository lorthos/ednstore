(ns cljtable.load-test
  (:require [clojure.test :refer :all]
            [cljtable.core :refer :all]))

(defn segment-fixture [f]
  (initialize!)
  (f)
  (stop!))

(use-fixtures :each segment-fixture)


(deftest load-test
  (testing "load-100k-items"
    (doseq [x (range 100000)]
      (let [v (str x (java.util.UUID/randomUUID))]
        (insert! x v)
        )
      )
    (is (string? (lookup 42)))
    )
  )

