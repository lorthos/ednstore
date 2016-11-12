(ns ednstore.store.segment
  "all segment management is handled here,
  additional logic about merging should be handled here"
  (:require
    [ednstore.common :as c]
    [ednstore.io.read :refer :all]
    [ednstore.io.write :as w]
    [ednstore.store.metadata :as md]
    [clojure.tools.logging :as log]))

(defrecord ActiveSegment
  [id index last-offset
   wc
   rc])

(defrecord ReadOnlySegment
  [id index rc])

(defrecord SegmentOperationLog
  [key old-offset new-offset op-type])

(defn make-new-segment!
  "make a new segment at the given path with the given id"
  [namespace id]
  (let [file (c/get-segment-file! namespace id)]
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
  [namespace id]
  (md/create-ns-metadata! namespace)
  (let [new-segment (make-new-segment! namespace id)
        old-active (md/get-active-segment-for-namespace namespace)]
    ;first redirect the writes
    ;point to new active segment
    (md/set-active-segment-for-ns! namespace new-segment)
    (log/debugf "rolled new segment, old active was %s" old-active)
    (when old-active
      (let [old-id (:id old-active)]

        ;close the old active segment
        (w/close!! (:wc old-active))
        (log/debugf "old segments before roll: %s" (md/get-old-segments namespace))
        (md/add-old-segment-for-ns! namespace old-id
                                    (map->ReadOnlySegment {:id    old-id
                                                           :index (:index old-active)
                                                           :rc    (:rc old-active)}))
        (log/debugf "old segments after roll: %s" (md/get-old-segments namespace))))
    (md/get-active-segment-for-namespace namespace))
  )