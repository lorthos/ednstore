(ns ednstore.store.writer-test
  (:require [clojure.test :refer :all]
            [ednstore.store.writer :refer :all]
            [ednstore.store.metadata :as md]
            [ednstore.store.segment :as s]))

(def test-db-ns "writer-test1")

(defn writer-fixture [f]
  (reset! md/store-meta {})
  (s/roll-new-segment! test-db-ns 0)
  (f)
  (s/close-segment! (md/get-active-segment-for-namespace test-db-ns)))

(use-fixtures :each writer-fixture)

;some serialization overhead with nippy
(deftest put-to-active-segment
  (testing "segment, index and offset management when writing"
    ;index last-offset read-chan write-chan
    (write! test-db-ns "A" "B")
    (write! test-db-ns "AAAA" "BBBB")
    ;validate index
    (is (= 2 (count (keys @(:index (md/get-active-segment-for-namespace test-db-ns))))))
    (is (= 72 @(:last-offset (md/get-active-segment-for-namespace test-db-ns))))
    (is (= 0 (get @(:index (md/get-active-segment-for-namespace test-db-ns)) "A")))
    (is (= 33 (get @(:index (md/get-active-segment-for-namespace test-db-ns)) "AAAA")))
    ))

(deftest delete-from-active-segment
  (testing "segment, index and offset management when writing"
    ;index last-offset read-chan write-chan
    (write! test-db-ns "A" "B")
    (write! test-db-ns "AAAA" "BBBB")
    ;validate index
    (is (= 2 (count (keys @(:index (md/get-active-segment-for-namespace test-db-ns))))))
    (delete! test-db-ns "A")
    (is (= 2 (count (keys @(:index (md/get-active-segment-for-namespace test-db-ns))))))
    ;TODO side-effect of deleting something that does not exist
    (delete! test-db-ns "NON_EXISTING_KEY" )
    (is (= 3 (count (keys @(:index (md/get-active-segment-for-namespace test-db-ns))))))
    )
  )
