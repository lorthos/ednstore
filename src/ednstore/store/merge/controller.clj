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
  [namespace id]
  (let [segment-file (c/get-segment-file! namespace id)]
    (log/infof "Deleting segment %s from disk" id)
    (io/delete-file segment-file)))

(defn merge!
  "merge 2 segments by
  active new segment and dropping old segments"
  [^String current-namespace
   ^Number older-segment-id
   ^Number newer-segment-id]
  (log/infof "Start merge! Namespace: %s old-segment: %s and new-segment: %s with status: %s ..."
             current-namespace
             older-segment-id
             newer-segment-id
             (md/get-all-md-for-ns current-namespace))
  (let [segment1 (get (md/get-old-segments current-namespace) older-segment-id)
        segment2 (get (md/get-old-segments current-namespace) newer-segment-id)]
    ;TODO background (c/do-sequential merger-exec)
    (let [merged-segment-id
          (m/make-merge! current-namespace segment1 segment2)]
      (log/infof "Segment merge complete for %s - %s . Created new segment: %s ",
                 segment1
                 segment2
                 merged-segment-id)
      (log/infof "Activating merged segment: %s  ", merged-segment-id)
      (let [loaded-segment (lo/load-read-only-segment current-namespace merged-segment-id)]
        (log/debugf "Loaded new segment: %s" loaded-segment)
        ;;TODO make atomic
        (md/disable-merged-segments current-namespace
                                    (:id segment1)
                                    (:id segment2))
        (log/infof "Removed old segments: %s" (md/get-all-md-for-ns current-namespace))
        (s/close-segment! segment1)
        (s/close-segment! segment2)
        (md/add-old-segment-for-ns! current-namespace merged-segment-id loaded-segment)
        (log/infof "Activated merged segment: %s  ", merged-segment-id)
        (log/infof "Deleting closed segments....")
        (delete-segment-from-disk! current-namespace (:id segment1))
        (delete-segment-from-disk! current-namespace (:id segment2))
        ))))

(defn attempt-merge-for-namespace! [current-namespace]
  (try
    (do
      (log/infof "Checking for merge: for ns %s" current-namespace))
    (let [mergeables
          (m/get-mergeable-segments
            (md/get-old-segments current-namespace)
            (:merge-strategy e/props))]
      (if mergeables
        (do
          (log/infof "Found mergeable segments: %s" mergeables)
          (merge! current-namespace
                  (:id (first mergeables))
                  (:id (second mergeables))))
        (log/infof "No candidates for segment merge ...")))
    (catch Exception e
      (log/errorf "Error in merger!: %s" (.getMessage e))
      (.printStackTrace e))))

(defn make-merge-func
  "TODO should merge across all namespaces"
  []
  (fn []
    (doall
      (map
        attempt-merge-for-namespace!
        (md/get-namespaces)))))

(defn make-merger-pool! [interval-sec]
  (let [pool
        (Executors/newScheduledThreadPool 1)]
    (log/infof "Starting merger thread...")
    (.scheduleAtFixedRate pool
                          (make-merge-func)
                          0 interval-sec TimeUnit/SECONDS)
    pool))
