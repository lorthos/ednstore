(ns ednstore.store.merge.controller-test
  (:require [clojure.test :refer :all]
            [ednstore.store.merge.controller :refer :all]
            [ednstore.store.segment :as seg :refer :all]
            [ednstore.store.writer :as w]
            [ednstore.store.segment :as s]
            [ednstore.store.loader :as lo]
            [ednstore.store.metadata :as md])
  (:import (java.io FileNotFoundException)))

(def test-db-ns "controller-test1")

(defn writer-fixture [f]
  (reset! md/store-meta {})
  (f))

(use-fixtures :each writer-fixture)

(deftest segment-merge-test
  (testing "merging of segments"
    (let [seg1
          (atom (seg/make-new-segment! test-db-ns 101))
          seg2
          (atom (seg/make-new-segment! test-db-ns 102))
          seg3
          (atom (seg/make-new-segment! test-db-ns 103))]

      (with-redefs-fn {#'md/get-active-segment-for-namespace (fn [_] @seg1)}
        #(do
           (w/write! test-db-ns "k1" "v1")
           (w/write! test-db-ns "k1" "v2")
           (w/write! test-db-ns "k2" "v1")))


      (with-redefs-fn {#'md/get-active-segment-for-namespace (fn [_] @seg2)}
        #(do
           (w/write! test-db-ns "k1" "v333")
           (w/write! test-db-ns "k1" "v444")
           (w/delete! test-db-ns "k2")))


      (with-redefs-fn {#'md/get-active-segment-for-namespace (fn [_] @seg3)}
        #(do
           (w/write! test-db-ns "k3" "v555")))


      (s/close-segment! @seg1)
      (s/close-segment! @seg2)
      (s/close-segment! @seg3)

      (reset! seg1 (lo/load-read-only-segment test-db-ns 101))
      (reset! seg2 (lo/load-read-only-segment test-db-ns 102))
      (reset! seg3 (lo/load-read-only-segment test-db-ns 103))

      (reset! md/store-meta
              {test-db-ns {:active-segment nil
                           :old-segments {101 @seg1
                                          102 @seg2
                                          103 @seg3}}})

      (merge! test-db-ns 101 102)
      (is (= '(103 100)
             (map :id
                  (vals (md/get-old-segments test-db-ns))))
          "old segments should be dropped and merged segment should be present"
          )

      (is (thrown? FileNotFoundException
                   (lo/load-read-only-segment test-db-ns 101))
          "segment should have been deleted from disk")

      (is (thrown? FileNotFoundException
                   (lo/load-read-only-segment test-db-ns 102))
          "segment should have been deleted from disk")
      )))
