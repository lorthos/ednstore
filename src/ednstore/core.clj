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

(defn load-existing-namespace! [current-namespace]
  (let [segment-ids (->> (str (:path e/props) current-namespace)
                         clojure.java.io/file
                         file-seq
                         (remove #(.isDirectory ^File %))
                         reverse
                         (map (comp read-string #(.substring % 0 (.lastIndexOf % ".")) #(.getName ^File %))))]
    (md/create-ns-metadata! current-namespace)
    (log/infof "About to load the following segments for namespace %s : %s "
               current-namespace
               segment-ids)
    (let [active-segment (s/roll-new-segment! current-namespace (inc (first segment-ids)))
          read-segments (zipmap segment-ids (doall (map #(ldr/load-read-only-segment
                                                           current-namespace
                                                           %) segment-ids)))]
      ;TODO shut down existing stuff first or check?
      (md/set-active-segment-for-ns! current-namespace active-segment)
      (md/set-old-segments-for-ns! current-namespace read-segments))
    ))

(defn init-new-namespace! [current-namespace]
  (md/create-ns-metadata! current-namespace)
  (let [active-segment (s/roll-new-segment! current-namespace 1000)]
    (md/set-active-segment-for-ns! current-namespace active-segment)))


(deftype SimpleDiskStore [] IKVStorage
  (insert! [this current-namespace k v]
    (log/debugf "write key: %s value: %s to namespace: %s"
                k v current-namespace)

    (if-not (md/get-active-segment-for-namespace current-namespace)
      (do
        (log/infof "Writing no non-existing namespace, initalizing with default settings")
        (init-new-namespace! current-namespace)))

    (if (< @(:last-offset (md/get-active-segment-for-namespace current-namespace))
           (:segment-roll-size e/props))
      (c/do-sequential @exec-pool
                       (wrt/write! current-namespace k v))
      (do
        (log/infof "Segment: %s has reached max size, rolling new"
                   (:id (md/get-active-segment-for-namespace current-namespace)))
        (s/roll-new-segment! current-namespace
                             (inc (:id (md/get-active-segment-for-namespace current-namespace)))))))

  (delete! [this current-namespace k]
    (c/do-sequential @exec-pool
                     (wrt/delete! current-namespace k)))

  (lookup
    [this current-namespace k]
    (rdr/read-all current-namespace k))

  (initialize! [this config]
    (.mkdir (io/file (:path config)))
    (let [existing-namespaces
          (into []
                (map
                  #(.getName %)
                  (-> "target/segments"
                      clojure.java.io/file
                      .listFiles)))]
      (log/infof "Initializing edn store with existing namespaces : %s" existing-namespaces)
      (doall
        (map
          load-existing-namespace!
          existing-namespaces)))
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
                                     (md/get-namespaces)))))
    (reset! md/store-meta {})
    ))

