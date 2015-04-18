(ns cljtable.store.reader
  (:require [cljtable.store.segment :as s])
  (:import (java.nio.channels SeekableByteChannel)
           (java.nio ByteBuffer)))

(defn read-int-from-chan [^SeekableByteChannel chan]
  (let [buf (ByteBuffer/allocate 4)]
    (.read chan buf)
    (.flip buf)
    (.getInt buf)))

(defn read-str-from-chan [^SeekableByteChannel chan length]
  (let [buf (ByteBuffer/allocate length)]
    (.read chan buf)
    (String. (.array buf) "UTF-8")))

(defn read-direct [^String key segment]
  (let [offset (get @(:index segment) key nil)
        chan (:read-chan segment)]
    (if offset
      (do
        (.position ^SeekableByteChannel chan offset)
        (let [kl (read-int-from-chan chan)
              k (read-str-from-chan chan kl)
              vl (read-int-from-chan chan)
              v (read-str-from-chan chan vl)]
          v))
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


