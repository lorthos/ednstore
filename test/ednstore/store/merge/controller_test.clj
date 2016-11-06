(ns ednstore.store.merge.controller-test
  (:require [clojure.test :refer :all]
            [ednstore.store.merge.controller :refer :all]
            [ednstore.store.segment :as seg :refer :all]
            [ednstore.store.writer :as w]
            [ednstore.store.segment :as s]
            [ednstore.store.loader :as lo])
  (:import (java.io FileNotFoundException)))


(deftest segment-merge-test
  (testing "merging of segments"
    (let [seg1
          (atom (seg/make-new-segment! 101))
          seg2
          (atom (seg/make-new-segment! 102))
          seg3
          (atom (seg/make-new-segment! 103))]

      (w/write! "k1" "v1" @seg1)
      (w/write! "k1" "v2" @seg1)
      (w/write! "k2" "v1" @seg1)

      (w/write! "k1" "v333" @seg2)
      (w/write! "k1" "v444" @seg2)
      (w/delete! "k2" @seg2)

      (w/write! "k3" "v555" @seg3)

      (s/close-segment! @seg1)
      (s/close-segment! @seg2)
      (s/close-segment! @seg3)

      (reset! seg1 (lo/load-read-only-segment 101))
      (reset! seg2 (lo/load-read-only-segment 102))
      (reset! seg3 (lo/load-read-only-segment 103))

      (let [segment-map (atom {101 @seg1
                               102 @seg2
                               103 @seg3})]
        (merge! segment-map 101 102)
        (println "***" @segment-map)
        (is (= '(103 100)
               (map :id
                    (vals @segment-map)))
            "old segments should be dropped and merged segment should be present"
            ))

      (is (thrown? FileNotFoundException
                   (lo/load-read-only-segment 101))
          "segment should have been deleted from disk")

      (is (thrown? FileNotFoundException
                   (lo/load-read-only-segment 102))
          "segment should have been deleted from disk")
      )))
