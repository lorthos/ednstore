(ns cljtable.core
  (:require [cljtable.store.common :as c]
            [cljtable.store.writer :as wrt]
            [cljtable.store.reader :as rdr]
            [cljtable.store.segment :as s]
            [cljtable.env :as e])
  (:import (java.util.concurrent Executors)
           (java.io File)))

(def exec (Executors/newSingleThreadExecutor))

(defn initialize!
  "initialize the store with the given config
  ;check storage
  ;init readonnly segments and active segments if possible"
  [config]
  (let [files (->> (:root-path e/props)
                   clojure.java.io/file
                   file-seq
                   (remove #(.isDirectory ^File %))
                   reverse
                   (map (comp read-string #(.substring % 0 (.lastIndexOf % ".")) #(.getName ^File %))))
        active-segment (s/make-active-segment! (first files))]
    (println files)
    (println (class files))
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