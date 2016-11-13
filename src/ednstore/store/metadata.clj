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

(defn create-ns-metadata! [^String current-namespace]
  (if-not (contains? @store-meta
                     current-namespace)
    (swap! store-meta assoc current-namespace {:active-segment nil
                                       :old-segments   {}})))

(defn get-active-segment-for-namespace
  [^String current-namespace]
  (let [ns-entry (get @store-meta current-namespace)]
    (if ns-entry
      (:active-segment ns-entry)
      nil)))

(defn get-old-segments [^String current-namespace]
  (:old-segments (get @store-meta current-namespace)))

(defn get-all-segments [^String current-namespace]
  (cons (get-active-segment-for-namespace current-namespace)
        (vals (get-old-segments current-namespace))))


(defn set-active-segment-for-ns! [^String current-namespace new-segment]
  (swap! store-meta assoc current-namespace {:active-segment new-segment
                                     :old-segments   (:old-segments (get @store-meta namespace))}))

(defn add-old-segment-for-ns! [^String current-namespace old-segment-id old-segment]
  (swap! store-meta assoc current-namespace {:active-segment (:active-segment (get @store-meta namespace))
                                     :old-segments   (assoc
                                                       (:old-segments (get @store-meta namespace))
                                                       old-segment-id
                                                       old-segment)}))

(defn set-old-segments-for-ns!
  "set all the old segments in 1 shot, used by the loader"
  [^String current-namespace old-segments]
  (swap! store-meta assoc current-namespace {:active-segment (:active-segment (get @store-meta current-namespace))
                                     :old-segments   old-segments}))

(defn disable-merged-segments
  "removes the given id's from old-segments structure"
  [^String current-namespace
   old-segment-id-1
   old-segment-id-2]
  (swap! store-meta assoc current-namespace {:active-segment (:active-segment (get @store-meta current-namespace))
                                     :old-segments   (dissoc
                                                       (:old-segments (get @store-meta current-namespace))
                                                       old-segment-id-1
                                                       old-segment-id-2)})
  )