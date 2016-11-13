(ns ednstore.core
  (:require [ednstore.common :as c]
            [ednstore.store.writer :as wrt]
            [ednstore.store.reader :as rdr]
            [ednstore.store.segment :as s]
            [ednstore.store.loader :as ldr]
            [ednstore.store.merge.controller :as mcon]
            [ednstore.env :as e]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [ednstore.store.metadata :as md])
  (:refer ednstore.common :only [IKVStorage])
  (:import (java.util.concurrent Executors TimeUnit)
           (java.io File)))

(def exec-pool
  "Main write thread, all writes are sequential"
  (atom nil))

(def merge-pool
  (atom nil))

(deftype SimpleDiskStore [] IKVStorage
  (insert! [this namespace k v]
    (if (< @(:last-offset (md/get-active-segment-for-namespace namespace))
           (:segment-roll-size e/props))
      (c/do-sequential @exec-pool
                       (wrt/write! namespace k v))
      (do
        (log/infof "Segment: %s has reached max size, rolling new"
                   (:id (md/get-active-segment-for-namespace namespace)))
        (s/roll-new-segment! namespace
                             (inc (:id (md/get-active-segment-for-namespace namespace)))))))

  (delete! [this namespace k]
    (c/do-sequential @exec-pool
                     (wrt/delete! namespace k)))

  (lookup
    [this namespace k]
    (rdr/read-all namespace k))

  (initialize! [this c]
    (.mkdir (io/file (:path c)))
    ;TODO for each ns at this level do the following
    (let [segment-ids (->> (:path c)
                           clojure.java.io/file
                           file-seq
                           (remove #(.isDirectory ^File %))
                           reverse
                           (map (comp read-string #(.substring % 0 (.lastIndexOf % ".")) #(.getName ^File %))))]
      (if-not (empty? segment-ids)
        (let [active-segment (s/roll-new-segment! (inc (first segment-ids)))
              read-segments (zipmap segment-ids (doall (map ldr/load-read-only-segment segment-ids)))]
          ;TODO shut down existing stuff first or check?
          (reset! s/active-segment active-segment)
          (reset! s/old-segments read-segments))
        (let [active-segment (s/roll-new-segment! 1000)]
          (reset! s/active-segment active-segment))))
    ;init the merger
    (reset! exec-pool (Executors/newSingleThreadExecutor))
    (reset! merge-pool
            (mcon/make-merger-pool! (:merge-trigger-interval-sec e/props)))
    )

  (stop!
    [this]
    (log/infof "Shutting down db...")
    (.shutdown @merge-pool)
    (.awaitTermination @merge-pool 1 TimeUnit/MINUTES)
    (.shutdown @exec-pool)
    (.awaitTermination @exec-pool 1 TimeUnit/MINUTES)
    (dorun (map s/close-segment! (flatten
                                   (map
                                     md/get-all-segments
                                     (md/get-namespaces)))))))

