(ns cljtable.store.writer
  (:require [nio.core :as nio]
            [cljtable.store.common :refer :all]
            [cljtable.store.segment]
            [cljtable.store.common :as c])
  (:import (java.nio ByteBuffer)
           (java.io ByteArrayOutputStream)
           (cljtable.store.segment ActiveSegment))
  )


(defn get-log-to-write
  "format will be: LENGTH:KEY:OP_TYPE:LENGTH:VALUE"
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

(defn get-log-to-delete
  "format will be: LENGTH:KEY:OP_TYPE"
  [#^bytes k]
  (let [out (ByteArrayOutputStream.)
        buf (ByteBuffer/allocate 4)
        kl (alength k)]
    (.write out (.array (.putInt buf kl)))
    (.write out k)
    (.clear buf)
    (.write out (byte 42))
    (.toByteArray out)))

(defn write!
  "write to the active segment only, should not write to an inactive segment
  active segment looks like this:
  index last-offset read-chan write-chan
  "
  [k v ^ActiveSegment segment]
  (let [key (field->wire k)
        val (field->wire v)
        barray (get-log-to-write key val)
        append-offset-length (alength barray)]
    ;update index
    ;update increment last offset
    ;write
    ;TODO atomic
    (swap! (:index segment) assoc k @(:last-offset segment))
    (swap! (:last-offset segment) + append-offset-length)
    (.write (:write-chan segment) (nio/byte-buffer barray))
    ;(.flush (:write-chan segment))
    )
  ;find the segments index
  ;segments current offset will be the index value
  ;append to file
  ;update index
  ;append segment offset counter
  )

(defn delete!
  "write to log with the delete marker"
  [k ^ActiveSegment segment]
  (let [barray (get-log-to-delete (c/field->wire k))
        append-offset-length (alength barray)]
    ;when deleting, now sure what to do with indexes here
    ;append to file
    ;update index
    ;append segment offset counter
    ;TODO atomic
    (swap! (:index segment) assoc k @(:last-offset segment))
    (swap! (:last-offset segment) + append-offset-length)
    (.write (:write-chan segment) (nio/byte-buffer barray))
    )
  )

