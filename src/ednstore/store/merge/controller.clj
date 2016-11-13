(ns ednstore.store.merge.controller
  (:require [clojure.tools.logging :as log]
            [ednstore.store.segment :as s]
            [ednstore.store.loader :as lo]
            [ednstore.store.merger :as m]
            [ednstore.common :as c]
            [clojure.java.io :as io]
            [ednstore.env :as e]
            [ednstore.store.metadata :as md])
  (:import (java.util.concurrent Executors TimeUnit)))

(defn delete-segment-from-disk!
  [table id]
  (let [segment-file (c/get-segment-file! table id)]
    (log/infof "Deleting segment %s from disk" id)
    (io/delete-file segment-file)))

(defn merge!
  "merge 2 segments by
  active new segment and dropping old segments"
  [^String table
   ^Number older-segment-id
   ^Number newer-segment-id]
  (log/infof "Start merge! table: %s old-segment: %s and new-segment: %s with status: %s ..."
             table
             older-segment-id
             newer-segment-id
             (md/get-all-md-for-table table))
  (let [segment1 (get (md/get-old-segments table) older-segment-id)
        segment2 (get (md/get-old-segments table) newer-segment-id)]
    ;TODO background (c/do-sequential merger-exec)
    (let [merged-segment-id
          (m/make-merge! table segment1 segment2)]
      (log/infof "Segment merge complete for %s - %s . Created new segment: %s ",
                 segment1
                 segment2
                 merged-segment-id)
      (log/infof "Activating merged segment: %s  ", merged-segment-id)
      (let [loaded-segment (lo/load-read-only-segment table merged-segment-id)]
        (log/debugf "Loaded new segment: %s" loaded-segment)
        ;;TODO make atomic
        (md/disable-merged-segments table
                                    (:id segment1)
                                    (:id segment2))
        (log/infof "Removed old segments: %s" (md/get-all-md-for-table table))
        (s/close-segment! segment1)
        (s/close-segment! segment2)
        (md/add-old-segment-for-table! table merged-segment-id loaded-segment)
        (log/infof "Activated merged segment: %s  ", merged-segment-id)
        (log/infof "Deleting closed segments....")
        (delete-segment-from-disk! table (:id segment1))
        (delete-segment-from-disk! table (:id segment2))
        ))))

(defn attempt-merge-for-table! [table]
  (try
    (do
      (log/infof "Checking for merge: for table %s" table))
    (let [mergeables
          (m/get-mergeable-segments
            (md/get-old-segments table)
            (:merge-strategy e/props))]
      (if mergeables
        (do
          (log/infof "Found mergeable segments: %s" mergeables)
          (merge! table
                  (:id (first mergeables))
                  (:id (second mergeables))))
        (log/infof "No candidates for segment merge ...")))
    (catch Exception e
      (log/errorf "Error in merger!: %s" (.getMessage e))
      (.printStackTrace e))))

(defn make-merge-func
  "TODO should merge across all tables"
  []
  (fn []
    (doall
      (map
        attempt-merge-for-table!
        (md/get-tables)))))

(defn make-merger-pool! [interval-sec]
  (let [pool
        (Executors/newScheduledThreadPool 1)]
    (log/infof "Starting merger thread...")
    (.scheduleAtFixedRate pool
                          (make-merge-func)
                          0 interval-sec TimeUnit/SECONDS)
    pool))
