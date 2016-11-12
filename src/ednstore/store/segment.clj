(ns ednstore.store.segment
  "all segment management is handled here,
  additional logic about merging should be handled here"
  (:require
    [ednstore.common :as c]
    [ednstore.io.read :refer :all]
    [ednstore.io.write :as w]))

(defonce old-segments (atom {}))
(defonce active-segment (atom nil))

(defrecord ActiveSegment
  [id index last-offset
   wc
   rc])

(defrecord ReadOnlySegment
  [id index rc])

(defrecord SegmentOperationLog
  [key old-offset new-offset op-type])

(defn get-all-segments []
  (cons @active-segment (vals @old-segments)))

(defn make-new-segment!
  "make a new segment at the given path with the given id"
  [id]
  (let [file (c/get-segment-file! id)]
    (map->ActiveSegment
      {:id          id
       :index       (atom {})
       :last-offset (atom 0)
       :wc          (w/make-write-channel! file)
       :rc          (make-read-channel! file)})))

(defn close-segment! [segment]
  (if (:wc segment)
    (w/close!! (:wc segment)))
  (close-read!! (:rc segment)))

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
            (w/close!! (:wc old-active))
            (swap! old-segments assoc old-id
                   (map->ReadOnlySegment {:id    old-id
                                          :index (:index old-active)
                                          :rc    (:rc old-active)}))))
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
  ;TODO merger implementation
  )
