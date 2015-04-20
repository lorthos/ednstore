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
    (wrt/write! "s0" "v0" @s/active-segment)
    (is (= "v0" (read-direct "s0" @s/active-segment)))
    (is (= "v0" (read-all "s0")))
    (let [old-segment-id (:id @s/active-segment)]
      (is (= 0 old-segment-id))
      (s/roll-new-segment! 1)
      (is (= 1 (:id @s/active-segment)))
      (is (true? (contains? @s/old-segments old-segment-id)))
      (is (nil? (read-direct "s0" @s/active-segment)))
      (is (= "v0" (read-direct "s0" (get @s/old-segments old-segment-id))))
      (is (= "v0" (read-all "s0")))
      (wrt/write! "s1" "v1" @s/active-segment)
      (is (= "v1" (read-all "s1")))
      (wrt/write! "s1" "v11" @s/active-segment)
      (is (= "v11" (read-all "s1")))
      (s/roll-new-segment! 2)
      (is (= 2 (:id @s/active-segment)))
      (is (= "v11" (read-all "s1")))
      (is (= "v0" (read-all "s0")))
      (wrt/write! "s2" "v2" @s/active-segment)
      (is (= "v2" (read-all "s2")))
      )

    )

  )
