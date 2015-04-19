(ns cljtable.store.reader
  (:require [cljtable.store.segment :as s]
            [cljtable.store.common :as c])
  (:import (java.nio.channels SeekableByteChannel)
           (java.nio ByteBuffer)))

(defn read-int-from-chan [^SeekableByteChannel chan]
  (let [buf (ByteBuffer/allocate 4)]
    (.read chan buf)
    (.flip buf)
    (.getInt buf)))

(defn read-nippy-from-chan [^SeekableByteChannel chan length]
  (let [buf (ByteBuffer/allocate length)]
    (.read chan buf)
    (c/wire->field (.array buf))))

(defn read-byte-from-chan [^SeekableByteChannel chan]
  (let [buf (ByteBuffer/allocate 1)]
    (.read chan buf)
    (.flip buf)
    (.get buf)))

(defn read-direct
  "should only read values that are not deleted
  old indexes might still contain deleted-record's key"
  [read-key segment]
  (let [offset (get @(:index segment) read-key nil)
        chan (:read-chan segment)]
    (if offset
      (do
        (.position ^SeekableByteChannel chan offset)
        (let [kl (read-int-from-chan chan)
              k (read-nippy-from-chan chan kl)
              op_type (read-byte-from-chan chan)]
          (if-not (= k read-key)
            (throw (RuntimeException. "segment key is different than index key, index is inconsistent")))
          (if (= op_type (byte 41))
            (let [vl (read-int-from-chan chan)
                  v (read-nippy-from-chan chan vl)]
              v))))
      nil)))


(defn lookup
  "search for the given key across all indexes of all segments"
  [^String key]
  (let [all-segments (cons @s/active-segment @s/old-segments)
        target-segment (first (filter #(contains? % key) all-segments))]

    (if target-segment
      (read-direct key @target-segment)
      ;indices are in decreasing order
      ;first match is the latest value
      ;TODO
      )
    ))


