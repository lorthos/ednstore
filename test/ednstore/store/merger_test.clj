(ns ednstore.store.merger-test
  (:require [clojure.test :refer :all]
            [ednstore.store.merger :refer :all]
            [ednstore.store.segment :as seg :refer :all]
            [ednstore.store.loader :as lo]
            [ednstore.store.writer :as w]
            [ednstore.store.reader :as r]
            [ednstore.env :as env]
            [clojure.java.io :as io]))


(deftest merge-candidates
  (testing "get merge candidates"
    (is (= '(1000 1001) (get-merge-candidate-ids '(1000 1001))))
    (is (= '(1000 1001) (get-merge-candidate-ids '(1000 1001 1002))))
    (is (= '(0 1002) (get-merge-candidate-ids '(0 1002))))
    (is (nil? (get-merge-candidate-ids '(0))))
    (is (nil? (get-merge-candidate-ids '(1002))))
    ))

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
          (atom (seg/make-new-segment! 600))
          new-seg
          (atom (seg/make-new-segment! 601))]
      (w/write! "k1" "v1" @old-seg)
      (w/write! "k1" "v2" @old-seg)
      (w/write! "k2" "v1" @old-seg)

      (w/write! "k1" "v333" @new-seg)
      (w/write! "k1" "v444" @new-seg)
      (w/delete! "k2" @new-seg)

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

      (reset! old-seg (lo/load-read-only-segment 600))
      (reset! new-seg (lo/load-read-only-segment 601))

      (make-merge! @old-seg
                   @new-seg)

      (seg/close-segment! @old-seg)
      (seg/close-segment! @new-seg)

      )))


(deftest merge-strategy-test
  (testing "merge streategy by size"
    (let [old-seg
          (atom (seg/make-new-segment! 600))
          new-seg
          (atom (seg/make-new-segment! 601))]

      (w/write! "k1" "v1" @old-seg)
      (w/write! "k1" "v2" @old-seg)
      (w/write! "k2" "v1" @old-seg)

      (w/write! "k1" "v333" @new-seg)
      (w/write! "k1" "v444" @new-seg)
      (w/delete! "k2" @new-seg)

      (is (= nil
             (get-mergeable-segment-ids [@old-seg @new-seg] {:min-size 1000000})))
      (is (= [600 601]
             (map :id
                  (get-mergeable-segment-ids [@old-seg @new-seg] {:min-size 10})))))))