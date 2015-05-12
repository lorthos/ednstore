(ns cljtable.store.segment
  "all segment management is handled here,
  additional logic about merging should be handled here"
  (:require [clojure.java.io :as io]
            [nio.core :as nio]
            [cljtable.store.common :as c])
  (:import (java.nio.channels WritableByteChannel SeekableByteChannel)))

(defonce old-segments (atom {}))

(defonce active-segment (atom nil))

;maybe this is not a good idea
;can create top level atom for active index and write channels
;TODO
(defrecord ActiveSegment [id index last-offset ^WritableByteChannel wc ^SeekableByteChannel rc])
(defrecord ReadOnlySegment [id index ^SeekableByteChannel rc])

(defn get-all-segments []
  (cons @active-segment (vals @old-segments)))

(defn make-new-segment!
  "make a new segment at the given path with the given id"
  [id]
  (let [file (c/get-segment-file! id)]
    (ActiveSegment. id (atom {}) (atom 0) (nio/writable-channel file) (nio/readable-channel file))))

(defn close-segment-fully! [segment]
  (if (:wc segment)
    (.close (:wc segment)))
  (.close (:rc segment)))

(defn roll-new-segment!
  "roll a new segment on the filesystem,
  update the current segment pointer to the newly created segment
  promote the old active segment to the old-segments collection.


  1.create new active segment
  2.point to new active segment
  3.close write channel of old active segment and create ReadOnlySegment
  4.move old active segment to old-segment list
  "
  [id]
  (let [segment (make-new-segment! id)]
    ;point to new active segment
    (if @active-segment
      (let [old-active @active-segment
            old-id (:id old-active)]
        (reset! active-segment segment)
        (if old-active
          (do
            (.close (:wc old-active))
            (swap! old-segments assoc old-id (ReadOnlySegment. old-id (:index old-active) (:rc old-active)))))
        )
      (reset! active-segment segment))
    )
  @active-segment
  )

(defn merge-segments!
  "merge two given segments together, creating a new segment containining
  cleaned up union of both
  update old-segments collection to point to newly merged segment and remove the old ones"
  [^ReadOnlySegment seg1 ^ReadOnlySegment seg2]
  ;TODO
  )
