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

(defn segment-has-key? [k segment]
  (if-not (nil? (:index segment))
    (let [index @(:index segment)]
      (if index
        (contains? index k)
        false))))

(defn read-all
  "search for the given key across all indexes of all segments"
  [k]
  (if (nil? @s/active-segment)
    (throw (RuntimeException. "active segment is null!")))
  (let [all-segments (cons @s/active-segment (vals @s/old-segments))
        target-segment (first (filter #(segment-has-key? k %) all-segments))]
    (println target-segment)
    (if target-segment
      (read-direct k target-segment)
      ;indices are in decreasing order
      ;first match is the latest value
      ;TODO
      )
    ))


