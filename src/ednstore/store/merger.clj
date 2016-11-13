(ns ednstore.store.merger
  (:require [ednstore.store.segment :as s]
            [ednstore.common :refer [->opts]]
            [ednstore.store.reader :as r]
            [ednstore.store.writer :as w]
            [clojure.tools.logging :as log]
            [ednstore.io.read :refer :all]
            [ednstore.store.metadata :as md])
  (:import (ednstore.store.segment ReadOnlySegment SegmentOperationLog)))

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
  (let [source-chan
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
  (log/debugf "Create oplog for old segment %s and new segment %s with args %s"
              old-segment new-segment (->opts args))
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

(defn pick-next-available-segment-id-for-merge
  "given a list of the old segments, decide on which segment it to use for the new merged segment"
  [active-segment old-segments]
  (let [old-segment-ids (map :id old-segments)
        all-ids (sort < (conj old-segment-ids (:id active-segment)))]
    (log/infof "old-segment-ids %s" old-segment-ids)
    (log/infof "all-ids %s" all-ids)
    (let [candidate (first
                      (for [a (range 0 (:id active-segment))
                            :when (not
                                    (some #(= a %) all-ids))]
                        a))]
      (log/infof "Using %s as next segment id for merge" candidate)
      (if (nil? candidate)
        (do
          (log/errorf "cannot proceed with merge, dont know how to create new id. used-ids: %s"
                      all-ids)
          (throw (RuntimeException. "cannot proceed with merge, dont know how to create new id")))
        candidate))))

(defn make-merge!
  "merge the two segments and return a new ReadOnlySegment

  Will run in a seperate single background thread"
  [^String table
   ^ReadOnlySegment older-segment
   ^ReadOnlySegment newer-segment]
  (log/debugf "Make-merge! oold-segment %s new-segment %s"
              older-segment
              newer-segment)
  (let [oplog (make-oplog-for-new-segment older-segment
                                          newer-segment)
        new-segment (s/make-new-segment! table
                                         (pick-next-available-segment-id-for-merge
                                           (md/get-active-segment-for-table table)
                                           (vals (md/get-old-segments table))))]
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
            (w/write-to-segment!
              (:key pair)
              (:val pair)
              new-segment))
          )
        oplog))
    (s/close-segment! new-segment)
    (log/debugf "segment after write: %s" (into {} new-segment))
    (:id new-segment)))

(defn get-mergeable-segments
  "executed periodically, triggers a merge if a merge is decided based on the current read-only segments.
  Should not start a merge on the active segment
  "
  [old-segments merge-strategy]
  (log/infof "Looking for mergeable segments in:  %s with strategy: %s"
             (keys old-segments)
             merge-strategy)
  (let [merge-candiates (take 2 (sort #(< (:id %1) (:id %2)) (vals old-segments)))
        merge-candiates-size (map #(size!! (:rc %)) merge-candiates)]
    (log/debugf "merge-candiates : %s" (into [] merge-candiates))
    (log/debugf "merge-candiates-size : %s" (into [] merge-candiates-size))
    (if (= 2
           (count (filter #(<
                             (:min-size merge-strategy)
                             %)
                          merge-candiates-size)))
      (into [] merge-candiates))))

