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

;"Main write thread, all writes are sequential"


(deftype SimpleDiskStore [exec-pool merge-pool] IKVStorage
  (insert! [this table k v]
    (log/tracef "write key: %s value: %s to table: %s"
                k v table)

    (if-not (md/get-active-segment-for-table table)
      (do
        (log/infof "Writing no non-existing table, initalizing with default settings")
        (wrt/init-new-table! table)))

    (if (< @(:last-offset (md/get-active-segment-for-table table))
           (:segment-roll-size e/props))
      (c/do-sequential exec-pool
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
    (c/do-sequential exec-pool
                     (wrt/delete! table k)))

  (lookup
    [this table k]
    (rdr/read-all table k))

  (initialize! [this config]
    (log/infof "Starting EdnStore with the following config: %s" config)
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
          ldr/load-existing-table!
          existing-tables)))
    (log/infof "Initializing thread pools .....")
    ;injected
    )

  (stop!
    [this]
    (log/infof "Shutting down db...")
    (.shutdown ^ExecutorService merge-pool)
    (.awaitTermination merge-pool 1 TimeUnit/MINUTES)
    (.shutdown ^ExecutorService exec-pool)
    (.awaitTermination exec-pool 1 TimeUnit/MINUTES)
    (dorun (map s/close-segment! (flatten
                                   (map
                                     md/get-all-segments
                                     (md/get-tables)))))
    (reset! md/store-meta {})
    ))

