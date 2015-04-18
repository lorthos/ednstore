(ns cljtable.store.writer-test
  (:require [clojure.test :refer :all]
            [cljtable.store.writer :refer :all]
            [cljtable.store.segment :as seg]
            [cljtable.store.reader :as rdr]
            [clojure.java.io :as io]
            [nio.core :as nio]
            ))

(def segment (atom nil))

(defn segment-fixture [f]
  (reset! segment (seg/make-active-segment! "test.tbl"))
  (f)
  (seg/close-active-segment! @segment))

(use-fixtures :each segment-fixture)


(deftest wire-format-test
  (testing "convert tuple to wire format"
    (let [k (.getBytes "A")
          v (.getBytes "B")
          wire (get-log-line k v)]
      (println (seq wire))
      (is (= 10 (alength wire)))
      (is (= '(0 0 0 1 65 0 0 0 1 66) (seq wire)))
      )
    (let [k (.getBytes "AA")
          v (.getBytes "BB")
          wire (get-log-line k v)]
      (println (seq wire))
      (is (= 12 (alength wire)))
      (is (= '(0 0 0 2 65 65 0 0 0 2 66 66) (seq wire)))
      )
    (let [k (.getBytes "A")
          v (.getBytes "ABCD")
          wire (get-log-line k v)]
      (println (seq wire))
      (is (= 13 (alength wire)))
      (is (= '(0 0 0 1 65 0 0 0 4 65 66 67 68) (seq wire)))
      )
    ))

(deftest put-to-active-segment
  (testing "segment, index and offset management when writing"
    ;index last-offset read-chan write-chan
    (write! "A" "B" @segment)                               ;offset 10
    (write! "AAAA" "BBBB" @segment)                         ;offset 10+16
    ;validate index
    (is (= 2 (count (keys @(:index @segment)))))
    (is (= 26 @(:last-offset @segment)))
    (println @(:index @segment))
    (is (= 0 (get @(:index @segment) "A")))
    (is (= 10 (get @(:index @segment) "AAAA")))
    ))
