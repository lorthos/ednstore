(ns ednstore.store.merge.controller
  (:require [clojure.tools.logging :as log]
            [ednstore.store.segment :as s]
            [ednstore.store.loader :as lo]
            [ednstore.store.merger :as m]
            [ednstore.common :as c])
  (:import (java.util.concurrent Executors)))

(def merger-exec
  "Main merge thread, merge operation is sequential at this time"
  (Executors/newSingleThreadExecutor))


(defn merge!
  "merge 2 segments by
  active new segment and dropping old segments"
  [segments-map-atom older-segment-id newer-segment-id]
  (let [segment1 (get @segments-map-atom older-segment-id)
        segment2 (get @segments-map-atom newer-segment-id)]
    (log/infof "Start segment merge %s and %s ..." segment1 segment2)
    ;TODO background (c/do-sequential merger-exec )
    (let [merged-segment-id
          (m/make-merge! segment1 segment2)]
      (log/infof "Segment merge complete for %s - %s . Created new segment: %s ",
                 segment1
                 segment2
                 merged-segment-id)
      (log/infof "Activating merged segment: %s  ", merged-segment-id)
      (let [loaded-segment (lo/load-read-only-segment merged-segment-id)]
        (log/debugf "Loaded new segment: %s" loaded-segment)
        ;;TODO make atomic
        (swap! segments-map-atom dissoc (:id segment1))
        (swap! segments-map-atom dissoc (:id segment2))
        (log/infof "Removed old segments: %s" @segments-map-atom)
        (s/close-segment! segment1)
        (s/close-segment! segment2)
        (swap! segments-map-atom assoc merged-segment-id loaded-segment)
        (log/infof "Activated merged segment: %s  ", merged-segment-id)
        )
      )
    ;once the new segment id is there, load that segment and delete the old 2 segments
    ))
