(ns cljtable.store.common
  "common utilities
  TODO: move serialization out of here"
  (:require [taoensso.nippy :as nippy]
            [clojure.java.io :as io]
            [cljtable.env :as e]))


(defn field->wire
  "converts the given key or value to byte-array"
  [key-or-value]
  (nippy/freeze key-or-value))


(defn wire->field
  "converts the given key or value to byte-array"
  [^bytes wire-formatted]
  (nippy/thaw wire-formatted))

(defmacro do-sequential [executor & body]
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
