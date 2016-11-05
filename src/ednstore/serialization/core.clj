(ns ednstore.serialization.core
  "serialization of key and value pairs"
  (:require [taoensso.nippy :as nippy])
  (:import (java.io ByteArrayOutputStream)
           (java.nio ByteBuffer)))


(defn field->wire
  "converts the given key or value to byte-array"
  [key-or-value]
  (nippy/freeze key-or-value))


(defn wire->field
  "converts the given key or value to byte-array"
  [^bytes wire-formatted]
  (nippy/thaw wire-formatted))


(defn create-append-log
  "helper function to create the byte arrays to be written as key and value
  format will be: LENGTH:KEY:OP_TYPE:LENGTH:VALUE"
  [#^bytes k #^bytes v]
  (let [out (ByteArrayOutputStream.)
        buf (ByteBuffer/allocate 4)
        kl (alength k)
        vl (alength v)]
    (.write out (.array (.putInt buf kl)))
    (.write out k)
    (.clear buf)
    (.write out (byte 41))
    (.write out (.array (.putInt buf vl)))
    (.write out v)
    (.clear buf)
    (.toByteArray out)))

(defn create-tombstone-log
  "helper function to create the byte arrays to be written as key and value when marking the record as deleted
  format will be: LENGTH:KEY:OP_TYPE"
  [#^bytes k]
  (let [out (ByteArrayOutputStream.)
        buf (ByteBuffer/allocate 4)
        kl (alength k)]
    (.write out (.array (.putInt buf kl)))
    (.write out k)
    (.clear buf)
    (.write out (byte 42))
    (.toByteArray out)))