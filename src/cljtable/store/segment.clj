(ns cljtable.store.segment
  "all segment management is handled here,
  additional logic about merging should be handled here"
  (:require [clojure.java.io :as io]
            [nio.core :as nio])
  (:import (java.nio.channels WritableByteChannel SeekableByteChannel)))

(def old-segments (atom {}))

(def active-segment (atom nil))

;maybe this is not a good idea
;can create top level atom for active index and write channels
;TODO
(defrecord ActiveSegment [index last-offset ^SeekableByteChannel read-chan ^WritableByteChannel write-chan])
(defrecord ReadOnlySegment [index ^SeekableByteChannel read-chan])

(defn make-active-segment! [path]
  (let [file (io/file path)]
    (ActiveSegment. (atom {}) (atom 0) (nio/readable-channel file) (nio/writable-channel file))))

(defn close-active-segment! [^ActiveSegment segment]
  (.close (:write-chan segment)))

(defn load-segments!
  "load segments from folder - make newest the active segment.
  should set old-segments and active-segment if possible"
  [path]
  ;TODO
  )

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
  (let [segment-file (io/file (str id ".tbl"))
        segment {:index      {}
                 :write-chan (nio/writable-channel segment-file)
                 :read-chan  (nio/readable-channel segment-file)}]
    ;point to new active segment

    (let [old-active @active-segment]
      (reset! active-segment segment)
      ;TODO close write channel of old-active
      ;(.close (:write-chan old-active))
      (swap! old-segments assoc (:id old-active) old-active)
      )
    )
  ;TODO
  )

(defn merge-segments!
  "merge two given segments together, creating a new segment containining
  cleaned up union of both
  update old-segments collection to point to newly merged segment and remove the old ones"
  [^ReadOnlySegment seg1 ^ReadOnlySegment seg2]
  ;TODO
  )
