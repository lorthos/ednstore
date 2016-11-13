(ns ednstore.store.writer-test
  (:require [clojure.test :refer :all]
            [ednstore.store.writer :refer :all]
            [ednstore.store.metadata :as md]
            [ednstore.store.segment :as s]))

(def test-db "writer-test1")

(defn writer-fixture [f]
  (reset! md/store-meta {})
  (s/roll-new-segment! test-db 0)
  (f)
  (s/close-segment! (md/get-active-segment-for-table test-db)))

(use-fixtures :each writer-fixture)

;some serialization overhead with nippy
(deftest put-to-active-segment
  (testing "segment, index and offset management when writing"
    ;index last-offset read-chan write-chan
    (write! test-db "A" "B")
    (write! test-db "AAAA" "BBBB")
    ;validate index
    (is (= 2 (count (keys @(:index (md/get-active-segment-for-table test-db))))))
    (is (= 72 @(:last-offset (md/get-active-segment-for-table test-db))))
    (is (= 0 (get @(:index (md/get-active-segment-for-table test-db)) "A")))
    (is (= 33 (get @(:index (md/get-active-segment-for-table test-db)) "AAAA")))
    ))

(deftest delete-from-active-segment
  (testing "segment, index and offset management when writing"
    ;index last-offset read-chan write-chan
    (write! test-db "A" "B")
    (write! test-db "AAAA" "BBBB")
    ;validate index
    (is (= 2 (count (keys @(:index (md/get-active-segment-for-table test-db))))))
    (delete! test-db "A")
    (is (= 2 (count (keys @(:index (md/get-active-segment-for-table test-db))))))
    ;TODO side-effect of deleting something that does not exist
    (delete! test-db "NON_EXISTING_KEY")
    (is (= 3 (count (keys @(:index (md/get-active-segment-for-table test-db))))))
    )
  )

(deftest custom-write-test
  (testing "writing to segment directly"
    (is (= 0 (count (keys @(:index (md/get-active-segment-for-table test-db))))))
    (write-to-segment! "CCCC" "CCCC" (md/get-active-segment-for-table test-db))
    (is (= 1 (count (keys @(:index (md/get-active-segment-for-table test-db))))))
    )

  )
