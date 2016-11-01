(ns cljtable.store.merger
  (:require [cljtable.store.segment :as s]
            [cljtable.common :as c])
  (:import (java.util.concurrent Executors)
           (cljtable.store.segment ReadOnlySegment)))

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

(defn make-merged-op-log [old-log new-log]
  (let [old-log
        (map #(assoc %
               :from
               :old) old-log)
        new-log
        (map #(assoc %
               :from
               :new) new-log)]
    (concat old-log new-log)))

(defn make-merge!
  "merge the two segments and return a new ReadOnlySegment

  Will run in a seperate single background thread"
  [^ReadOnlySegment older-segment
   ^ReadOnlySegment newer-segment]
  (let [added-keys (atom '())
        deleted-keys (atom '())]
    ;TODO
    )
  ;both segments has indices
  ;for each key in index1
  ;walk on the newer segment
  ;added '()
  ;deleted '()
  ;
  ;walk on the older segment
  ;append to added or deleted if it does not exist

  )

(defn reduce-segment
  "reduce 2 segment logs into 1"
  [segment-seq-old segment-seq-new]
  ;TODO reduce each segment by loading into a key-offset map
  ;reduce 2 segments together
  )
(defn merge!
  [older-segment-id newer-segment-id]
  (let [segment1 (get @s/old-segments older-segment-id)
        segment2 (get @s/old-segments newer-segment-id)]
    (c/do-sequential merger-exec (make-merge! segment1 segment2))
    )
  )
