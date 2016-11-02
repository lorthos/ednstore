(ns cljtable.store.reader
  (:require [cljtable.store.segment :as s :refer :all]
            [cljtable.io.core :as io]
            [clojure.tools.logging :as log])
  (:import (java.nio.channels SeekableByteChannel)
           (java.util Iterator)
           (cljtable.store.segment SegmentOperationLog)))

(defn read-kv
  "read the next key value pair starting with the given offset from the channel"
  [^SeekableByteChannel chan offset]
  (do
    (log/debugf "seek to position %s for channel: %s" offset chan)
    (.position chan offset)
    (let [kl (io/read-int-from-chan chan)
          k (io/read-type-from-chan chan kl)
          op_type (io/read-byte-from-chan chan)]
      (log/infof "Read key length: %s key: %s op-type: %s" kl k op_type)
      (if (= op_type (byte 41))
        (let [vl (io/read-int-from-chan chan)
              v (io/read-type-from-chan chan vl)]
          {:key k :val v})))))

(defn read-direct
  "should only read values that are not deleted
  old indexes might still contain deleted-record's key"
  [read-key segment]
  (let [offset (get @(:index segment) read-key nil)
        chan ^SeekableByteChannel (:rc segment)]
    (if offset
      (:val (read-kv chan offset))
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

(defn ^SegmentOperationLog read-log-operation!
  "given a channel that is at the end position of a record (or at the beginning of the file)
  1. reads the length of the key
  2. reads the key
  3. reads the operation type
  4. if update - reads the length of the value
  5. reads the value
  6. calculates total bytes read returns the key and new offset"
  [chan offset-atom]
  (let [old-offset @offset-atom
        kl (io/read-int-from-chan chan)
        k (io/read-type-from-chan chan kl)
        op_type (io/read-byte-from-chan chan)]
    (if (= op_type (byte 41))
      (let [vl (io/read-int-from-chan chan)
            v (io/read-type-from-chan chan vl)]
        (do
          (swap! offset-atom + 4 kl 1 4 vl)
          (map->SegmentOperationLog
            {:key     k :old-offset old-offset :new-offset @offset-atom
             :op-type op_type})))
      (do
        (swap! offset-atom + 4 kl 1)
        (map->SegmentOperationLog
          {:key     k :old-offset old-offset :new-offset @offset-atom
           :op-type op_type})))))

(defn segment->seq
  "return a lazy sequence of operations
  that were used to create the segment

  will read evey operation including the deletion markers,
  does not read the value"
  [^SeekableByteChannel read-channel]
  (.position read-channel 0)
  (let [current (atom 0)
        end (.size read-channel)]
    (iterator-seq
      (reify Iterator
        (hasNext [this] (< @current end))
        (next [this]
          (read-log-operation!
            read-channel
            current))))))