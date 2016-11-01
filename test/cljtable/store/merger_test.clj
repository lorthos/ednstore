(ns cljtable.store.merger-test
  (:require [clojure.test :refer :all]
            [cljtable.store.merger :refer :all]
            [cljtable.store.segment :as s :refer :all]
            [cljtable.store.reader :as r]))


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


(defn create-merged-segment-from-oplog
  [merged-oplog old-segment new-segment]
  ;now based on the op, go read the value and append to new segment
  ;TODO resume
  )


;(let [old-segment (get @s/old-segments 0)
;      new-segment (get @s/old-segments 1)]
;  (create-merged-segment-from-oplog
;    (cleanup-log
;      (make-merged-op-log
;        (r/segment->seq (:rc old-segment))
;        (r/segment->seq (:rc new-segment))))
;    old-segment
;    new-segment
;    ))
