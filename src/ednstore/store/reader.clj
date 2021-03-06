(ns ednstore.store.reader
  (:require [ednstore.store.segment :as s :refer :all]
            [ednstore.io.read :refer :all]
            [clojure.tools.logging :as log]
            [ednstore.store.metadata :as md])
  (:import
    (java.util Iterator)
    (ednstore.store.segment SegmentOperationLog)))

(defn read-block! [chan
                   start-offset
                   read-val?]
  (let [kl (read-int!! chan)
        k (read-wire-format!! chan kl)
        op_type (read-byte!! chan)]
    (if (= op_type (byte 41))
      (let [vl (read-int!! chan)
            v (read-wire-format!! chan vl)]
        {:key     k :old-offset start-offset :new-offset (+ start-offset 4 kl 1 4 vl)
         :op-type op_type
         :value   (if read-val?
                    v
                    nil)})
      {:key     k :old-offset start-offset :new-offset (+ start-offset 4 kl 1)
       :op-type op_type})))

(defn read-kv
  "read the next key value pair starting with the given offset from the channel"
  [chan offset]
  (do
    (log/debugf "seek to position %s for channel: %s" offset chan)
    (position!! chan offset)
    (let [block (read-block! chan offset true)]
      (if (= (:op-type block) (byte 41))
        {:key (:key block) :val (:value block)}))))

(defn read-direct
  "should only read values that are not deleted
  old indexes might still contain deleted-record's key"
  [read-key segment]
  (let [offset (get @(:index segment) read-key nil)
        chan (:rc segment)]
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
  [table k]
  (if (nil? (md/get-active-segment-for-table table))
    (throw (RuntimeException. "active segment is null!")))
  (let [all-segments (md/get-all-segments table)
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
  (let [block (read-block! chan @offset-atom false)]
    (reset! offset-atom (:new-offset block))
    (dissoc block :value)))

(defn segment->seq
  "return a lazy sequence of operations
  that were used to create the segment

  will read evey operation including the deletion markers,
  does not read the value"
  [read-channel]
  (position!! read-channel 0)
  (let [current (atom 0)
        end (size!! read-channel)]
    (iterator-seq
      (reify Iterator
        (hasNext [this] (< @current end))
        (next [this]
          (read-log-operation!
            read-channel
            current))))))