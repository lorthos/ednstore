(ns cljtable.store.reader
  (:require [cljtable.store.segment :as s]
            [cljtable.io.core :as io])
  (:import (java.nio.channels SeekableByteChannel)))

(defn read-direct
  "should only read values that are not deleted
  old indexes might still contain deleted-record's key"
  [read-key segment]
  (let [offset (get @(:index segment) read-key nil)
        chan ^SeekableByteChannel (:rc segment)]
    (if offset
      (do
        (.position chan offset)
        (let [kl (io/read-int-from-chan chan)
              k (io/read-nippy-from-chan chan kl)
              op_type (io/read-byte-from-chan chan)]
          (if-not (= k read-key)
            (throw (RuntimeException. "segment key is different than index key, index is inconsistent")))
          (if (= op_type (byte 41))
            (let [vl (io/read-int-from-chan chan)
                  v (io/read-nippy-from-chan chan vl)]
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
  (let [all-segments (s/get-all-segments)
        target-segment (first (filter #(segment-has-key? k %) all-segments))]
    (if target-segment
      (read-direct k target-segment)
      ;indices are in decreasing order
      ;first match is the latest value
      ;TODO
      )))





