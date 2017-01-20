(ns ednserver.core-test
  (:require [clojure.test :refer :all]
            [ednserver.core :as c])
  (:import (ednstore.core SimpleDiskStore)))


(deftest simple-functionality-test
  (testing "with different types"
    (c/-main )
    )
  )