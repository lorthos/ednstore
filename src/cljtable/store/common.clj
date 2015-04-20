(ns cljtable.store.common
  (:require [taoensso.nippy :as nippy])
  (:import (java.util.concurrent Executor)))


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
