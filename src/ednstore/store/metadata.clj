(ns ednstore.store.metadata)

(defonce store-meta
         (atom {}))
;one key per namespace
;old-segments , active segment per key

(defn get-active-segment-for-namespace
  [namespace]
  (:active-segment (get @store-meta namespace)))

(defn get-old-segments [namespace]
  (:old-segments (get @store-meta namespace)))

(defn get-all-segments [namespace]
  (cons (get-active-segment-for-namespace namespace)
        (vals (get-old-segments namespace))))
