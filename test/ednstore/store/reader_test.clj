(ns ednstore.store.reader-test
  (:require [clojure.test :refer :all]
            [ednstore.store.reader :refer :all]
            [ednstore.store.segment :as s :refer :all]
            [ednstore.store.writer :as wrt]
            [ednstore.store.metadata :as md]))

(def test-db-ns "reader-test1")

(defn reader-fixture [f]
  (reset! md/store-meta {})
  (s/roll-new-segment! test-db-ns 0)
  (f)
  (close-segment! (md/get-active-segment-for-namespace test-db-ns)))

(use-fixtures :each reader-fixture)

(deftest read-from-segment
  (testing "read key from segment file"
    (wrt/write! test-db-ns "A" "B")
    (wrt/write! test-db-ns "AAAA" "BBBB")
    (is (= "B" (read-direct "A" (md/get-active-segment-for-namespace test-db-ns))))
    (is (= "BBBB" (read-direct "AAAA" (md/get-active-segment-for-namespace test-db-ns)))))
  (testing "read when key does not exist"
    (is (nil? (read-direct "NON_EXISTING_KEY" (md/get-active-segment-for-namespace test-db-ns)))))
  (testing "read when key is marked as deleted"
    (is (= "BBBB" (read-direct "AAAA" (md/get-active-segment-for-namespace test-db-ns))))
    (wrt/delete! test-db-ns "AAAA")
    (is (nil? (read-direct "AAAA" (md/get-active-segment-for-namespace test-db-ns)))))
  (testing "when key is updated"
    (wrt/write! test-db-ns "UPDATE" "V1")
    (is (= "V1" (read-direct "UPDATE" (md/get-active-segment-for-namespace test-db-ns))))
    (wrt/write! test-db-ns "UPDATE" "V2")
    (is (= "V2" (read-direct "UPDATE" (md/get-active-segment-for-namespace test-db-ns))))
    (wrt/delete! test-db-ns "UPDATE")
    (is (nil? (read-direct "UPDATE" (md/get-active-segment-for-namespace test-db-ns))))
    )
  (testing "test read from all segments"
    (wrt/write! test-db-ns "s0" "v0")
    (is (= "v0" (read-direct "s0" (md/get-active-segment-for-namespace test-db-ns))))
    (is (= "v0" (read-all test-db-ns "s0")))
    (let [old-segment-id (:id (md/get-active-segment-for-namespace test-db-ns))]
      (is (= 0 old-segment-id))
      (s/roll-new-segment! test-db-ns 1)
      (is (= 1 (:id (md/get-active-segment-for-namespace test-db-ns))))
      (is (true? (contains? (md/get-old-segments test-db-ns) old-segment-id)))
      (is (nil? (read-direct "s0" (md/get-active-segment-for-namespace test-db-ns))))
      (println
        "****"
        (md/get-old-segments test-db-ns))
      (is (= "v0" (read-direct "s0" (get (md/get-old-segments test-db-ns) old-segment-id))))
      (is (= "v0" (read-all test-db-ns "s0")))
      (wrt/write! test-db-ns "s1" "v1")
      (is (= "v1" (read-all test-db-ns "s1")))
      (wrt/write! test-db-ns "s1" "v11")
      (is (= "v11" (read-all test-db-ns "s1")))
      (s/roll-new-segment! test-db-ns 2)
      (is (= 2 (:id (md/get-active-segment-for-namespace test-db-ns))))
      (is (= "v11" (read-all test-db-ns "s1")))
      (is (= "v0" (read-all test-db-ns "s0")))
      (wrt/write! test-db-ns "s2" "v2")
      (is (= "v2" (read-all test-db-ns "s2")))))
  (testing "segment as collection of operations"
    (is (= '({:key        "A"
              :new-offset 33
              :old-offset 0
              :op-type    41}
              {:key        "AAAA"
               :new-offset 72
               :old-offset 33
               :op-type    41}
              {:key        "AAAA"
               :new-offset 92
               :old-offset 72
               :op-type    42}
              {:key        "UPDATE"
               :new-offset 131
               :old-offset 92
               :op-type    41}
              {:key        "UPDATE"
               :new-offset 170
               :old-offset 131
               :op-type    41}
              {:key        "UPDATE"
               :new-offset 192
               :old-offset 170
               :op-type    42}
              {:key        "s0"
               :new-offset 227
               :old-offset 192
               :op-type    41})
           (->> (get (md/get-old-segments test-db-ns) 0)
                :rc
                segment->seq
                (map #(into {} %)))))

    (is (= '({:key        "s1"
              :new-offset 35
              :old-offset 0
              :op-type    41}
              {:key        "s1"
               :new-offset 71
               :old-offset 35
               :op-type    41})
           (->> (get (md/get-old-segments test-db-ns) 1)
                :rc
                segment->seq
                (map #(into {} %)))))

    (is (= [{:key        "s2"
             :new-offset 35
             :old-offset 0
             :op-type    41}]
           (->> (md/get-active-segment-for-namespace test-db-ns)
                :rc
                segment->seq
                (map #(into {} %)))))
    )
  )

