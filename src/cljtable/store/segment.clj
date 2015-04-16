(ns cljtable.store.segment
  "all segment management is handled here,
  additional logic about merging should be handled here"
  )

(def old-segments (atom []))

(def active-segment (atom nil))

;maybe this is not a good idea
;can create top level atom for active index and write channels
;TODO
(defrecord ActiveSegment [index read-chan write-chan])
(defrecord ReadOnlySegment [index read-chan])

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
  ;TODO
  )

(defn merge-segments!
  "merge two given segments together, creating a new segment containining
  cleaned up union of both
  update old-segments collection to point to newly merged segment and remove the old ones"
  [^ReadOnlySegment seg1 ^ReadOnlySegment seg2]
  ;TODO
  )
