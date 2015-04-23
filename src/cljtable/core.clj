(ns cljtable.core
  (:require [cljtable.store.common :as c]
            [cljtable.store.writer :as wrt]
            [cljtable.store.reader :as rdr]
            [cljtable.store.segment :as s]
            [cljtable.store.loader :as ldr]
            [cljtable.env :as e]
            [nio.core :as nio])
  (:import (java.util.concurrent Executors)
           (java.io File)))

(def exec (Executors/newSingleThreadExecutor))

(defn load-segments!
  "load segments from folder - make newest the active segment.
  should set old-segments and active-segment if possible"
  [path]
  ;TODO
  )

(defn initialize!
  "initialize the store with the given config
  ;check storage
  ;init readonnly segments and active segments if possible"
  [config]
  (let [segment-ids (->> (:root-path e/props)
                         clojure.java.io/file
                         file-seq
                         (remove #(.isDirectory ^File %))
                         reverse
                         (map (comp read-string #(.substring % 0 (.lastIndexOf % ".")) #(.getName ^File %))))
        active-segment (ldr/load-active-segment (first segment-ids))
        read-segments (zipmap (rest segment-ids) (doall (map ldr/load-read-only-segment (rest segment-ids))))]
    (println segment-ids)
    (println active-segment)
    (println read-segments)
    ;TODO shut down existing stuff first or check?
    (reset! s/active-segment active-segment)
    (reset! s/old-segments read-segments)
    )
  ;TODO
  ;go to config folder
  ;get the list of segments
  ;make newest active
  ;make olders read only
  ;

  )

(defn stop!
  "close all open file handles"
  []
  ;TODO
  )

(defn insert!
  "should always be single threaded"
  [k v]
  (c/do-sequential exec (wrt/write! k v @s/active-segment))
  )

(defn delete!
  "same applies, all writes are sequential"
  [k]
  (c/do-sequential exec (wrt/delete! k @s/active-segment))
  )

(defn lookup
  [k]
  (rdr/read-all k))