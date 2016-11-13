(ns ednstore.store.metadata)

(defonce store-meta
         (atom {}))
;one key per namespace
;old-segments , active segment per key

(defn get-all-md-for-ns
  "return the current metadata state for given segment, mainly used for debugging"
  [namespace]
  (get @store-meta namespace))

(defn get-namespaces []
  (keys @store-meta))

(defn get-active-segment-for-namespace
  [^String namespace]
  (:active-segment (get @store-meta namespace)))

(defn get-old-segments [^String namespace]
  (:old-segments (get @store-meta namespace)))

(defn get-all-segments [^String namespace]
  (cons (get-active-segment-for-namespace namespace)
        (vals (get-old-segments namespace))))

(defn create-ns-metadata! [^String namespace]
  (if-not (contains? @store-meta
                     namespace)
    (swap! store-meta assoc namespace {:active-segment nil
                                       :old-segments   {}})))

(defn set-active-segment-for-ns! [^String namespace new-segment]
  (swap! store-meta assoc namespace {:active-segment new-segment
                                     :old-segments   (:old-segments (get @store-meta namespace))}))

(defn add-old-segment-for-ns! [^String namespace old-segment-id old-segment]
  (swap! store-meta assoc namespace {:active-segment (:active-segment (get @store-meta namespace))
                                     :old-segments   (assoc
                                                       (:old-segments (get @store-meta namespace))
                                                       old-segment-id
                                                       old-segment)}))

(defn disable-merged-segments
  "removes the given id's from old-segments structure"
  [^String namespace
   old-segment-id-1
   old-segment-id-2]
  (swap! store-meta assoc namespace {:active-segment (:active-segment (get @store-meta namespace))
                                     :old-segments   (dissoc
                                                       (:old-segments (get @store-meta namespace))
                                                       old-segment-id-1
                                                       old-segment-id-2)})
  )