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

(defn create-ns-metadata! [namespace]
  (swap! store-meta assoc namespace {:active-segment nil
                                     :old-segments {}}))

(defn set-active-segment-for-ns! [namespace new-segment]
  (swap! store-meta assoc namespace {:active-segment new-segment
                                     :old-segments   (:old-segments (get @store-meta namespace))}))

(defn add-old-segment-for-ns! [namespace old-segment-id old-segment]
  (swap! store-meta assoc namespace {:active-segment (:active-segment (get @store-meta namespace))
                                     :old-segments   (assoc (:old-segments (get @store-meta namespace))
                                                       old-segment-id
                                                       old-segment)}))