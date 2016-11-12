(ns ednstore.core
  (:require [ednstore.common :as c]
            [ednstore.store.writer :as wrt]
            [ednstore.store.reader :as rdr]
            [ednstore.store.segment :as s]
            [ednstore.store.loader :as ldr]
            [ednstore.store.merge.controller :as mcon]
            [ednstore.env :as e]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:refer ednstore.common :only [IKVStorage])
  (:import (java.util.concurrent Executors)
           (java.io File)))

(def exec
  "Main write thread, all writes are sequential"
  (Executors/newSingleThreadExecutor))

(def merge-pool
  (atom nil))

(deftype SimpleDiskStore [] IKVStorage
  (insert! [this k v]
    (if (< @(:last-offset @s/active-segment)
           (:segment-roll-size e/props))
      (c/do-sequential exec (wrt/write! k v @s/active-segment))
      (do
        (log/infof "Segment: %s has reached max size, rolling new" (:id @s/active-segment))
        (s/roll-new-segment! (inc (:id @s/active-segment)))
        )))

  (delete! [this k]
    (c/do-sequential exec (wrt/delete! k @s/active-segment)))

  (lookup
    [this k]
    (rdr/read-all k))

  (initialize! [this c]
    (.mkdir (io/file (:path c)))
    ;TODO check for clean init
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
    (reset! merge-pool
            (mcon/make-merger-pool! (:merge-trigger-interval-sec e/props)
                                    s/old-segments))
    )

  (stop!
    [this]
    (log/infof "Shutting down db...")
    (.shutdownNow @merge-pool)
    (dorun (map s/close-segment! (s/get-all-segments)))
    (reset! s/active-segment nil)
    (reset! s/old-segments {})
    ))

