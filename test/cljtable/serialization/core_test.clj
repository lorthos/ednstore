(ns cljtable.serialization.core-test
  (:require [clojure.test :refer :all]
            [cljtable.serialization.core :refer :all]))


(deftest wire-format-test
  (testing "convert tuple to wire format"
    (let [k (.getBytes "A")
          v (.getBytes "B")
          wire (get-log-to-write k v)]
      (println (seq wire))
      (is (= 11 (alength wire)))
      (is (= '(0 0 0 1 65 41 0 0 0 1 66) (seq wire)))
      )
    (let [k (.getBytes "AA")
          v (.getBytes "BB")
          wire (get-log-to-write k v)]
      (println (seq wire))
      (is (= 13 (alength wire)))
      (is (= '(0 0 0 2 65 65 41 0 0 0 2 66 66) (seq wire)))
      )
    (let [k (.getBytes "A")
          v (.getBytes "ABCD")
          wire (get-log-to-write k v)]
      (println (seq wire))
      (is (= 14 (alength wire)))
      (is (= '(0 0 0 1 65 41 0 0 0 4 65 66 67 68) (seq wire))))))
