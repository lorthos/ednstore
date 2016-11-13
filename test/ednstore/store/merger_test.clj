(ns ednstore.store.merger-test
  (:require [clojure.test :refer :all]
            [ednstore.store.merger :refer :all]
            [ednstore.store.segment :as seg :refer :all]
            [ednstore.store.loader :as lo]
            [ednstore.store.writer :as w]
            [ednstore.store.reader :as r]
            [ednstore.store.metadata :as md]))

(def test-db "merger-test1")

(def log1
  [{:key        "A"
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
    :op-type    42}])

(deftest log-cleanup
  (testing "merging of 2 log sequences"
    (is (= '({:key        "A"
              :new-offset 33
              :old-offset 0
              :op-type    41}
              {:key        "AAAA"
               :new-offset 92
               :old-offset 72
               :op-type    42})
           (cleanup-log log1)))))


(def clean-log-old
  [{:key        "B"
    :new-offset 33
    :old-offset 0
    :op-type    41}
   {:key        "AAAA"
    :new-offset 92
    :old-offset 72
    :op-type    42}])

(def clean-log-new
  [{:key        "A"
    :new-offset 33
    :old-offset 0
    :op-type    41}
   {:key        "AAAA"
    :new-offset 92
    :old-offset 72
    :op-type    42}])



(deftest merge-op
  (testing "basic merge"
    (is (= '({:from       :old
              :key        "B"
              :new-offset 33
              :old-offset 0
              :op-type    41}
              {:from       :old
               :key        "AAAA"
               :new-offset 92
               :old-offset 72
               :op-type    42}
              {:from       :new
               :key        "A"
               :new-offset 33
               :old-offset 0
               :op-type    41}
              {:from       :new
               :key        "AAAA"
               :new-offset 92
               :old-offset 72
               :op-type    42})
           (make-merged-op-log clean-log-old
                               clean-log-new)))
    (is (= [{:from       :old
             :key        "B"
             :new-offset 33
             :old-offset 0
             :op-type    41}
            {:from       :new
             :key        "AAAA"
             :new-offset 92
             :old-offset 72
             :op-type    42}
            {:from       :new
             :key        "A"
             :new-offset 33
             :old-offset 0
             :op-type    41}]
           (cleanup-log
             (make-merged-op-log clean-log-old
                                 clean-log-new)))))
  )

(deftest merging-test
  (testing "creating 2 custom segments an merging them"
    (let [old-seg
          (atom (seg/make-new-segment! test-db 600))
          new-seg
          (atom (seg/make-new-segment! test-db 601))]

      (with-redefs-fn {#'md/get-active-segment-for-table (fn [_] @old-seg)}
        #(do
           (w/write! test-db "k1" "v1")
           (w/write! test-db "k1" "v2")
           (w/write! test-db "k2" "v1")))

      (with-redefs-fn {#'md/get-active-segment-for-table (fn [_] @new-seg)}
        #(do
           (w/write! test-db "k1" "v333")
           (w/write! test-db "k1" "v444")
           (w/delete! test-db "k2")))



      (is (= '({:from       :old
                :key        "k1"
                :new-offset 35
                :old-offset 0
                :op-type    41}
                {:from       :old
                 :key        "k1"
                 :new-offset 70
                 :old-offset 35
                 :op-type    41}
                {:from       :old
                 :key        "k2"
                 :new-offset 105
                 :old-offset 70
                 :op-type    41}
                {:from       :new
                 :key        "k1"
                 :new-offset 37
                 :old-offset 0
                 :op-type    41}
                {:from       :new
                 :key        "k1"
                 :new-offset 74
                 :old-offset 37
                 :op-type    41}
                {:from       :new
                 :key        "k2"
                 :new-offset 92
                 :old-offset 74
                 :op-type    42})
             (make-merged-op-log
               (map #(into {} %) (r/segment->seq (:rc @old-seg)))
               (map #(into {} %) (r/segment->seq (:rc @new-seg)))))
          "verify oplog failed")

      (is (= '({:from       :new
                :key        "k1"
                :new-offset 74
                :old-offset 37
                :op-type    41})
             (map #(into {} %) (make-oplog-for-new-segment @old-seg
                                                           @new-seg
                                                           :filter-tombstones true)))
          "mergable oplog should only have 1 key (latest) since the other key has been deleted")

      (is (= '({:from       :new
                :key        "k1"
                :new-offset 74
                :old-offset 37
                :op-type    41}
                {:from       :new
                 :key        "k2"
                 :new-offset 92
                 :old-offset 74
                 :op-type    42})
             (map #(into {} %) (make-oplog-for-new-segment @old-seg
                                                           @new-seg)))
          "mergable oplog should keep tombstones")

      (is (= {:key "k1"
              :val "v444"}
             (read-oplog-item (first
                                (make-oplog-for-new-segment
                                  @old-seg
                                  @new-seg
                                  :filter-tombstones true))
                              @old-seg
                              @new-seg))

          "read the oplog item from its original segment")

      (is (= {:key "k1"
              :val "v1"}
             (read-oplog-item
               (map->SegmentOperationLog
                 {:from       :old
                  :key        "k1"
                  :new-offset 35
                  :old-offset 0
                  :op-type    41})
               @old-seg
               @new-seg))

          "read the oplog item from its original segment")

      (seg/close-segment! @old-seg)
      (seg/close-segment! @new-seg)

      (reset! old-seg (lo/load-read-only-segment test-db 600))
      (reset! new-seg (lo/load-read-only-segment test-db 601))

      ;return some mock id so that we pick another one
      (with-redefs-fn {#'md/get-active-segment-for-table (fn [_] {:id 602})}
        #(make-merge! test-db
                      @old-seg
                      @new-seg))

      (seg/close-segment! @old-seg)
      (seg/close-segment! @new-seg)

      )))


(deftest merge-strategy-test
  (testing "merge streategy by size"
    (let [seg1
          (atom (seg/make-new-segment! test-db 101))
          seg2
          (atom (seg/make-new-segment! test-db 102))
          seg3
          (atom (seg/make-new-segment! test-db 103))]


      (with-redefs-fn {#'md/get-active-segment-for-table (fn [_] @seg1)}
        #(do
           (w/write! test-db "k1" "v1")
           (w/write! test-db "k1" "v2")
           (w/write! test-db "k2" "v1")))

      (with-redefs-fn {#'md/get-active-segment-for-table (fn [_] @seg2)}
        #(do
           (w/write! test-db "k1" "v333")
           (w/write! test-db "k1" "v444")
           (w/delete! test-db "k2")))

      (with-redefs-fn {#'md/get-active-segment-for-table (fn [_] @seg3)}
        #(do
           (w/write! test-db "k3" "v555")))


      (is (= nil
             (get-mergeable-segments {101 @seg1
                                      102 @seg2
                                      103 @seg3} {:min-size 1000000})))
      (is (= [101 102]
             (map :id
                  (get-mergeable-segments {101 @seg1
                                           102 @seg2
                                           103 @seg3} {:min-size 10})))))))


(deftest pick-next-segment-id-form-merge
  (testing "next available segment id"
    (is (= 0
           (pick-next-available-segment-id-for-merge {:id 105}
                                                     [{:id 100}
                                                      {:id 101}
                                                      {:id 102}
                                                      {:id 103}
                                                      {:id 104}])))
    (is (= 2
           (pick-next-available-segment-id-for-merge {:id 3}
                                                     [{:id 0}
                                                      {:id 1}])))

    (is
      (thrown?
        RuntimeException
        (pick-next-available-segment-id-for-merge {:id 3}
                                                  [{:id 0}
                                                   {:id 1}
                                                   {:id 2}])))
    )
  )