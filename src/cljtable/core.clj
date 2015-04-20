(ns cljtable.core
  (:require [cljtable.store.common :as c]
            [cljtable.store.writer :as wrt]
            [cljtable.store.reader :as rdr]
            [cljtable.store.segment :as s])
  (:import (java.util.concurrent Executors)))

(def exec (Executors/newSingleThreadExecutor))

(defn initialize!
  "initialize the store with the given config
  ;check storage
  ;init readonnly segments and active segments if possible"
  [config]
  ;TODO
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

(defn delete! [k]
  ;TODO
  )

(defn lookup
  [k]
  (rdr/read-all k))