(ns cljtable.serialization.core
  "serialization of key and value pairs"
  (:require [taoensso.nippy :as nippy]))


(defn field->wire
  "converts the given key or value to byte-array"
  [key-or-value]
  (nippy/freeze key-or-value))


(defn wire->field
  "converts the given key or value to byte-array"
  [^bytes wire-formatted]
  (nippy/thaw wire-formatted))

