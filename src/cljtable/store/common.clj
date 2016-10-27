(ns cljtable.store.common
  "common utilities"
  (:require
    [clojure.java.io :as io]
    [cljtable.env :as e]))

(defmacro do-sequential
  "submit the expression to the sequential executor"
  [executor & body]
  `(.get (.submit ~executor (proxy [Callable] []
                              (call []
                                (do ~@body))))))


(defn get-segment-file!
  "based on the segment id and configured folder, get the full file"
  [id]
  (let [root-path (:path e/props)
        file (io/file (str root-path id ".tbl"))]
    (io/make-parents file)
    file))
