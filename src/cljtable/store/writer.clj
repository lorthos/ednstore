(ns cljtable.store.writer
  (:require [nio.core :as nio]
            [clojure.java.io :as io]
            [cljtable.store.segment])
  (:import (java.nio ByteBuffer)
           (java.io ByteArrayOutputStream)
           (cljtable.store.segment ActiveSegment))
  )


(defn get-log-line
  "format will be: LENGTH:KEY:LENGTH:VALUE"
  [#^bytes k #^bytes v]
  (let [out (ByteArrayOutputStream.)
        key-size (ByteBuffer/allocate 4)
        kl (alength k)
        vl (alength v)
        value-size (ByteBuffer/allocate 4)]
    (.write out (.array (.putInt key-size kl)))
    (.write out k)
    (.write out (.array (.putInt value-size vl)))
    (.write out v)
    (.toByteArray out)))

(defn write!
  "write to the active segment only, should not write to an inactive segment
  active segment looks like this:
  index last-offset read-chan write-chan
  "
  [^String k ^String v ^ActiveSegment segment]
  (let [key (.getBytes k "UTF-8")
        val (.getBytes v "UTF-8")
        barray (get-log-line key val)
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


(defn append! [segment])

