(ns ednstore.common
  (:require [clojure.java.io :as io]
            [ednstore.env :as e]))


(defprotocol IKVStorage
  (initialize! [this config])
  (stop! [this])

  (insert! [this k v])
  (delete! [this k])
  (lookup [this k]))

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


(defn ->opts
  "Coerces arguments to a map"
  [args]
  (let [x (first args)]
    (if (map? x)
      x
      (apply array-map args))))