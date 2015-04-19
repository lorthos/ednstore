(ns cljtable.store.reader-test
  (:require [clojure.test :refer :all]
            [cljtable.store.reader :refer :all]
            [cljtable.store.segment :as s]
            [cljtable.store.writer :as wrt]))



(defn segment-fixture [f]
  (s/roll-new-segment! 0)
  (f)
  (s/close-active-segment! @s/active-segment))

(use-fixtures :each segment-fixture)

(deftest read-from-segment
  (testing "read key from segment file"
    (wrt/write! "A" "B" @s/active-segment)
    (wrt/write! "AAAA" "BBBB" @s/active-segment)
    (is (= "B" (read-direct "A" @s/active-segment)))
    (is (= "BBBB" (read-direct "AAAA" @s/active-segment))))
  (testing "read when key does not exist"
    (is (nil? (read-direct "NON_EXISTING_KEY" @s/active-segment))))
  (testing "read when key is marked as deleted"
    (is (= "BBBB" (read-direct "AAAA" @s/active-segment)))
    (wrt/delete! "AAAA" @s/active-segment)
    (is (nil? (read-direct "AAAA" @s/active-segment))))
  (testing "when key is updated"
    (wrt/write! "UPDATE" "V1" @s/active-segment)
    (is (= "V1" (read-direct "UPDATE" @s/active-segment)))
    (wrt/write! "UPDATE" "V2" @s/active-segment)
    (is (= "V2" (read-direct "UPDATE" @s/active-segment)))
    (wrt/delete! "UPDATE" @s/active-segment)
    (is (nil? (read-direct "UPDATE" @s/active-segment)))
    )
  (testing "test read from all segments"
    (wrt/write! "ALL" "VALL" @s/active-segment)
    (is (= "VALL" (read-direct "ALL" @s/active-segment)))
    (is (= "VALL" (read-all "ALL")))
    (let [old-segment-id (:id @s/active-segment)]
      (is (= 0 old-segment-id))
      (s/roll-new-segment! 1)
      (is (= 1 (:id @s/active-segment)))
      (is (true? (contains? @s/old-segments old-segment-id)))
      (is (nil? (read-direct "ALL" @s/active-segment)))
      (is (= "VALL" (read-direct "ALL" (get @s/old-segments old-segment-id))))
      (is (= "VALL" (read-all "ALL")))
      )

    )

  )
