(ns ednstore.store.metadata)

(defonce store-meta
         (atom {}))

(defn get-all-md-for-table
  "return the current metadata state for given segment, mainly used for debugging"
  [table]
  (get @store-meta table))

(defn get-tables []
  (keys @store-meta))

(defn create-ns-metadata! [^String table]
  (if-not (contains? @store-meta
                     table)
    (swap! store-meta assoc table {:active-segment nil
                                   :old-segments   {}})))

(defn get-active-segment-for-table
  [^String table]
  (let [tbl-entry (get @store-meta table)]
    (if tbl-entry
      (:active-segment tbl-entry)
      nil)))

(defn get-old-segments [^String table]
  (:old-segments (get @store-meta table)))

(defn get-all-segments [^String table]
  (cons (get-active-segment-for-table table)
        (vals (get-old-segments table))))


(defn set-active-segment-for-table! [^String table new-segment]
  (swap! store-meta assoc table {:active-segment new-segment
                                 :old-segments   (:old-segments (get @store-meta table))}))

(defn add-old-segment-for-table! [^String table old-segment-id old-segment]
  (swap! store-meta assoc table {:active-segment (:active-segment (get @store-meta table))
                                 :old-segments   (assoc
                                                   (:old-segments (get @store-meta table))
                                                   old-segment-id
                                                   old-segment)}))

(defn set-old-segments-for-table!
  "set all the old segments in 1 shot, used by the loader"
  [^String table old-segments]
  (swap! store-meta assoc table {:active-segment (:active-segment (get @store-meta table))
                                 :old-segments   old-segments}))

(defn disable-merged-segments
  "removes the given id's from old-segments structure"
  [^String table
   old-segment-id-1
   old-segment-id-2]
  (swap! store-meta assoc table {:active-segment (:active-segment (get @store-meta table))
                                 :old-segments   (dissoc
                                                   (:old-segments (get @store-meta table))
                                                   old-segment-id-1
                                                   old-segment-id-2)})
  )