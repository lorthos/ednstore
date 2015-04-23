(ns cljtable.store.segment
  "all segment management is handled here,
  additional logic about merging should be handled here"
  (:require [clojure.java.io :as io]
            [nio.core :as nio]
            [cljtable.store.common :as c])
  (:import (java.nio.channels WritableByteChannel SeekableByteChannel)))

(def old-segments (atom {}))

(def active-segment (atom nil))

;maybe this is not a good idea
;can create top level atom for active index and write channels
;TODO
(defrecord ActiveSegment [id index last-offset ^WritableByteChannel write-chan ^SeekableByteChannel read-chan])
(defrecord ReadOnlySegment [id index ^SeekableByteChannel read-chan])

(defn make-active-segment!
  "make a new segment at the given path with the given id"
  ([id]
   (let [file (c/get-segment-file id)]
     (ActiveSegment. id (atom {}) (atom 0) (nio/writable-channel file) (nio/readable-channel file))))
  ([id file index offset read-chan]
   (ActiveSegment. id (atom index) (atom offset) (nio/writable-channel file) read-chan))
  )

(defn close-active-segment! [^ActiveSegment segment]
  (.close (:write-chan segment)))

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
  (let [segment (make-active-segment! id)]
    ;point to new active segment
    (if @active-segment
      (let [old-active @active-segment
            old-id (:id old-active)]
        (reset! active-segment segment)
        ;TODO close write channel of old-active
        (.close (:write-chan old-active))
        (swap! old-segments assoc old-id (ReadOnlySegment. old-id (:index old-active) (:read-chan old-active)))
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
