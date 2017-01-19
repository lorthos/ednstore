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
  (:import (java.util.concurrent Executors TimeUnit ExecutorService)
           (java.io File)))

(def exec-pool
  "Main write thread, all writes are sequential"
  (atom nil))

(def merge-pool
  (atom nil))

(defn load-existing-table! [table]
  (let [segment-ids (->> (str (:path e/props) table)
                         clojure.java.io/file
                         file-seq
                         (remove #(.isDirectory ^File %))
                         reverse
                         (map (comp read-string #(.substring % 0 (.lastIndexOf % ".")) #(.getName ^File %))))]
    (md/create-ns-metadata! table)
    (log/infof "About to load the following segments for table %s : %s "
               table
               segment-ids)
    (let [active-segment (s/roll-new-segment! table (inc (first segment-ids)))
          read-segments (zipmap segment-ids (doall (map #(ldr/load-read-only-segment
                                                           table
                                                           %) segment-ids)))]
      ;TODO shut down existing stuff first or check?
      (md/set-active-segment-for-table! table active-segment)
      (md/set-old-segments-for-table! table read-segments))
    ))

(defn init-new-table! [table]
  (md/create-ns-metadata! table)
  (let [active-segment (s/roll-new-segment! table 1000)]
    (md/set-active-segment-for-table! table active-segment)))


(deftype SimpleDiskStore [] IKVStorage
  (insert! [this table k v]
    (log/tracef "write key: %s value: %s to table: %s"
                k v table)

    (if-not (md/get-active-segment-for-table table)
      (do
        (log/infof "Writing no non-existing table, initalizing with default settings")
        (init-new-table! table)))

    (if (< @(:last-offset (md/get-active-segment-for-table table))
           (:segment-roll-size e/props))
      (c/do-sequential @exec-pool
                       (wrt/write! table k v))
      (do
        (log/infof "Segment: %s has reached max size, rolling new"
                   (:id (md/get-active-segment-for-table table)))
        (if (< (count (md/get-old-segments table)) (:max-segments e/props))
          (s/roll-new-segment! table
                               (inc (:id (md/get-active-segment-for-table table))))
          (log/warnf "Skipped rolling segment, reached max segments"))
        )))

  (delete! [this table k]
    (c/do-sequential @exec-pool
                     (wrt/delete! table k)))

  (lookup
    [this table k]
    (rdr/read-all table k))

  (initialize! [this config]
    (.mkdir (io/file (:path config)))
    (let [existing-tables
          (into []
                (map
                  #(.getName %)
                  (-> (:path config)
                      clojure.java.io/file
                      .listFiles)))]
      (log/infof "Initializing edn store with existing tables : %s" existing-tables)
      (doall
        (map
          load-existing-table!
          existing-tables)))
    (log/infof "Initializing thread pools .....")
    ;init the merger
    (reset! exec-pool (Executors/newSingleThreadExecutor))
    (reset! merge-pool
            (mcon/make-merger-pool! (:merge-trigger-interval-sec e/props)))
    )

  (stop!
    [this]
    (log/infof "Shutting down db...")
    (.shutdown ^ExecutorService @merge-pool)
    (.awaitTermination @merge-pool 1 TimeUnit/MINUTES)
    (.shutdown ^ExecutorService @exec-pool)
    (.awaitTermination @exec-pool 1 TimeUnit/MINUTES)
    (dorun (map s/close-segment! (flatten
                                   (map
                                     md/get-all-segments
                                     (md/get-tables)))))
    (reset! md/store-meta {})
    ))

