(ns cljtable.store.reader
  (:require [cljtable.store.segment :as s]))

(defn get
  "search for the given key across all indexes of all segments"
  [^String key]
  (let [all-indices (conj [] (:index @s/active-segment) (map :index @s/old-segments))]
    ;indices are in decreasing order
    ;first match is the latest value
    ;TODO
    )
  )
