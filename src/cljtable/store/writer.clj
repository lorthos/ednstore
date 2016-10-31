(ns cljtable.store.writer
  (:require [nio.core :as nio]
            [cljtable.store.segment]
            [cljtable.serialization.core :as ser])
  (:import (java.nio ByteBuffer)
           (java.io ByteArrayOutputStream)
           (cljtable.store.segment ActiveSegment)))


(defn get-log-to-write
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

(defn get-log-to-delete
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

(defn write!
  "write to the active segment only, should not write to an inactive segment
  active segment looks like this:
  index last-offset read-chan write-chan

  1.update index
  2.update increment last offset
  3.write
  "
  [k v ^ActiveSegment segment]
  (let [key (ser/field->wire k)
        val (ser/field->wire v)
        barray (get-log-to-write key val)
        append-offset-length (alength barray)]

    ;TODO should be atomic
    (swap! (:index segment) assoc k @(:last-offset segment))
    (swap! (:last-offset segment) + append-offset-length)
    (.write (:wc segment) (nio/byte-buffer barray))
    ;(.flush (:wc segment))
    )
  )

(defn delete!
  "write to log with the delete marker
  1.append to file
  2.update index
  3.append segment offset counter"
  [k ^ActiveSegment segment]
  (let [barray (get-log-to-delete (ser/field->wire k))
        append-offset-length (alength barray)]
    ;TODO atomic
    (swap! (:index segment) assoc k @(:last-offset segment))
    (swap! (:last-offset segment) + append-offset-length)
    (.write (:wc segment) (nio/byte-buffer barray))
    )
  )

