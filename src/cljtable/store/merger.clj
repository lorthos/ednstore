(ns cljtable.store.merger
  (:require [cljtable.store.segment :as s]
            [cljtable.common :as c]
            [cljtable.store.reader :as r]
            [cljtable.store.writer :as w]
            [clojure.tools.logging :as log])
  (:import (java.util.concurrent Executors)
           (cljtable.store.segment ReadOnlySegment SegmentOperationLog)
           (java.nio.channels SeekableByteChannel WritableByteChannel)))

(def merger-exec (Executors/newSingleThreadExecutor))

(defn get-merge-candidate-ids
  "get the id of the segments to be merged

  1000 1001 1002 (1003)->
  0 1002 (1003->
  1 (1003)

  "
  [read-only-segments]
  ;must have at least 2 active segments
  (when (>= (count read-only-segments) 2)
    (take 2 (sort < read-only-segments))))

(defn cleanup-log
  "given a log sequence, reduce it in a way that we end up with the
  latest operation for each key
  should be limited by the key cardinality"
  [log-seq]
  (let [merge-map (atom {})]
    (doall
      (map
        #(swap! merge-map assoc (:key %) %)
        log-seq))
    (vals
      @merge-map)))

(defn make-merged-op-log
  "merge 2 oplogs, associating a new field to indicate which log its coming from"
  [old-log new-log]
  (let [old-log
        (map #(assoc %
               :from
               :old) old-log)
        new-log
        (map #(assoc %
               :from
               :new) new-log)]
    (concat old-log new-log)))

(defn read-oplog-item
  "given a single merged oplog item and 2 segments,
  read the record fully including the value"
  [^SegmentOperationLog oplog
   old-segment
   new-segment]
  (log/debugf "reading oplog item from disk: %s" oplog)
  (let [^SeekableByteChannel source-chan
        (if (= :old (:from oplog))
          (:rc old-segment)
          (:rc new-segment))
        beginning (:old-offset oplog)]
    (log/debugf "read parameters %s %s" source-chan beginning)
    (r/read-kv source-chan beginning)))


(defn make-oplog-for-new-segment
  "the oplog for the segment to be written does not need to contain the deletion marker?"
  [old-segment new-segment]
  (filter #(= 41 (:op-type %))
          (cleanup-log
            (make-merged-op-log
              (r/segment->seq (:rc old-segment))
              (r/segment->seq (:rc new-segment))))))

(defn make-merge!
  "merge the two segments and return a new ReadOnlySegment

  Will run in a seperate single background thread"
  [^ReadOnlySegment older-segment
   ^ReadOnlySegment newer-segment]
  (let [oplog (make-oplog-for-new-segment older-segment
                                          newer-segment)
        new-segment (s/make-new-segment! 666)]
    (log/debugf "Read Oplog: %s" (into [] oplog))
    (log/debugf "segment created: %s" (into {} new-segment))
    (dorun
      (map
        (fn [oplog-item]
          (log/debugf "Read the following oplog item %s" (into {} oplog-item))
          (let [pair (read-oplog-item oplog-item
                                      older-segment
                                      newer-segment)]
            (log/debugf "Read the following pair %s" pair)
            (w/write! (:key pair)
                      (:val pair)
                      new-segment))
          )
        oplog))
    ;(.flush ^WritableByteChannel (:wc new-segment))
    (s/close-segment! new-segment)
    (log/debugf "segment after write: %s" (into {} new-segment))
    ))

(defn merge!
  [older-segment-id newer-segment-id]
  (let [segment1 (get @s/old-segments older-segment-id)
        segment2 (get @s/old-segments newer-segment-id)]
    (c/do-sequential merger-exec (make-merge! segment1 segment2))
    )
  )
