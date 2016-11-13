(ns ednstore.store.loader-test
  (:require [clojure.test :refer :all]
            [ednstore.store.loader :refer :all]
            [ednstore.store.segment :as s]
            [ednstore.store.writer :as wrt]
            [ednstore.store.metadata :as md]))

(def test-db "loader-test1")

(defn loader-fixture [f]
  (reset! md/store-meta {})
  (s/roll-new-segment! test-db 0)
  (f)
  (s/close-segment! (md/get-active-segment-for-table test-db)))

(use-fixtures :each loader-fixture)

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
    (wrt/write! test-db "A" "B")
    (wrt/write! test-db "B" "C")
    (wrt/write! test-db "C" "D")

    (is (= {:index {"A" 0 "B" 33 "C" 66} :offset 99}
           (load-index (:rc (md/get-active-segment-for-table test-db)))))
    (wrt/write! test-db "DD" "EE")
    (wrt/write! test-db "E" "F")

    (is (= {:index {"A" 0 "B" 33 "C" 66 "DD" 99 "E" 134} :offset 167}
           (load-index (:rc (md/get-active-segment-for-table test-db)))))
    (wrt/delete! test-db "E")

    (is (= {:index {"A" 0 "B" 33 "C" 66 "DD" 99 "E" 167} :offset 184}
           (load-index (:rc (md/get-active-segment-for-table test-db)))))

    (wrt/delete! test-db "DD")
    (is (= {:index {"A" 0 "B" 33 "C" 66 "DD" 184 "E" 167} :offset 202}
           (load-index (:rc (md/get-active-segment-for-table test-db)))))

    )
  )
