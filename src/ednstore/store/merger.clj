(ns ednstore.store.merger
  (:require [ednstore.store.segment :as s]
            [ednstore.common :as c :refer [->opts]]
            [ednstore.store.reader :as r]
            [ednstore.store.writer :as w]
            [clojure.tools.logging :as log])
  (:import (java.util.concurrent Executors)
           (ednstore.store.segment ReadOnlySegment SegmentOperationLog)
           (java.nio.channels SeekableByteChannel WritableByteChannel)))

(def merger-exec
  "Main merge thread, merge operation is sequential at this time"
  (Executors/newSingleThreadExecutor))

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
  "the oplog for the segment to be written does not need to contain the deletion marker?
  filtering out tombstones should only be done in the case that we are merging first 2 read-only segments
  "
  [old-segment new-segment & args]
  (let [filter-tombstones? (:filter-tombstones (->opts args))
        cleaned-log
        (cleanup-log
          (make-merged-op-log
            (r/segment->seq (:rc old-segment))
            (r/segment->seq (:rc new-segment))))]
    (if filter-tombstones?
      (filter #(= 41 (:op-type %))
              cleaned-log)
      cleaned-log)))

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


(defn get-mergeable-segment-ids
  "executed periodically, triggers a merge if a merge is decided based on the current read-only segments.
  Should not start a merge on the active segment
  "
  [old-segments merge-strategy]
  (let [merge-candiates (take 2 old-segments)
        merge-candiates-size (map #(.size (:rc %)) old-segments)]
    (log/debugf "Old segments size : %s" (into [] merge-candiates-size))
    (if (= 2
           (count (filter #(<
                             (:min-size merge-strategy)
                             %)
                          merge-candiates-size)))
      merge-candiates)))