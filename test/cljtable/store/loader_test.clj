(ns cljtable.store.loader-test
  (:require [clojure.test :refer :all]
            [cljtable.store.loader :refer :all]
            [cljtable.store.segment :as s]
            [cljtable.store.writer :as wrt]))

(defn segment-fixture [f]
  (s/roll-new-segment! 0)
  (f)
  (s/close-segment-fully! @s/active-segment)
  (reset! s/active-segment nil))

(use-fixtures :each segment-fixture)

(deftest append-next-line-to-index
  (testing "with read operation"
    (let [index (atom {})]
      ;with an artificial record of k1:v1
      (is (= 13 (append-next-line-to-index! index {:op :assoc :key "k1" :old-offset 0 :new-offset 13})))
      (is (= {"k1" 0} @index))
      ;k2:v2
      (is (= 26 (append-next-line-to-index! index {:op :assoc :key "k2" :old-offset 13 :new-offset 26})))
      (is (= {"k1" 0 "k2" 13} @index))
      ;k1:DELETE
      (is (= 33 (append-next-line-to-index! index {:op :dissoc :key "k1" :old-offset 26 :new-offset 33})))
      (is (= {"k1" 26 "k2" 13} @index))
      )
    )
  )


;TODO will fail when serialization function changes
(deftest reconstruct-index
  (testing "reading-from-actual-segment"
    (wrt/write! "A" "B" @s/active-segment)
    (wrt/write! "B" "C" @s/active-segment)
    (wrt/write! "C" "D" @s/active-segment)

    (is (= {:index {"A" 0 "B" 33 "C" 66} :offset 99}
           (load-index (:rc @s/active-segment))))
    (wrt/write! "DD" "EE" @s/active-segment)
    (wrt/write! "E" "F" @s/active-segment)

    (is (= {:index {"A" 0 "B" 33 "C" 66 "DD" 99 "E" 134} :offset 167}
           (load-index (:rc @s/active-segment))))
    (wrt/delete! "E" @s/active-segment)

    (is (= {:index {"A" 0 "B" 33 "C" 66 "DD" 99 "E" 167} :offset 184}
           (load-index (:rc @s/active-segment))))

    (wrt/delete! "DD" @s/active-segment)
    (is (= {:index {"A" 0 "B" 33 "C" 66 "DD" 184 "E" 167} :offset 202}
           (load-index (:rc @s/active-segment))))

    )
  )
