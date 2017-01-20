(ns ednstore.store.loader
  (:require [ednstore.common :as c]
            [ednstore.store.segment :refer :all]
            [ednstore.io.read :refer :all]
            [ednstore.store.reader :as rdr]
            [clojure.tools.logging :as log]
            [ednstore.env :as e]
            [ednstore.store.metadata :as md]
            [ednstore.store.segment :as s])
  (:import (java.io File)))

(defn read-next-key-and-offset-and-increment!
  "given a channel that is at the end position of a record (or at the beginning of the file)
  1. reads the length of the key
  2. reads the key
  3. reads the operation type
  4. if update - reads the length of the value
  5. reads the value
  6. calculates total bytes read returns the key and new offset"
  [chan offset-atom]
  (let [block (rdr/read-block! chan @offset-atom false)]
    (reset! offset-atom (:new-offset block))
    (dissoc block :value :op-type)))

(defn append-next-line-to-index!
  "given an index in the atom, append the read result to the index and return the latest offset"
  [index-atom read-result]
  (swap! index-atom assoc (:key read-result) (:old-offset read-result))
  (:new-offset read-result))

(defn load-index
  "goes through all the keys and re-construct {:key offset} hash index
  1. position the chan to the beginning offset
  2. read and assoc accordingly"
  [chan]
  (position!! chan 0)
  (let [offset-atom (atom 0)
        end (size!! chan)
        index (atom {})]
    (loop [current 0]
      (if (= current end)
        {:index @index :offset @offset-atom}
        (recur (append-next-line-to-index!
                 index
                 (read-next-key-and-offset-and-increment! chan offset-atom)))))))

(defn load-read-only-segment
  "given the segment id, load it as a read only segment"
  [table id]
  (log/debugf "Loading read only segment for ns: %s with id: %s" table id)
  (let [segment-file (c/get-segment-file! table id)
        read-chan (make-read-channel! segment-file)
        loaded (load-index read-chan)]
    (map->ReadOnlySegment
      {:id    id
       :index (atom (:index loaded))
       :rc    read-chan})))


(defn load-existing-table! [table]
  (let [segment-ids (->> (str (:path e/props) table)
                         clojure.java.io/file
                         file-seq
                         (remove #(.isDirectory ^File %))
                         reverse
                         (map (comp read-string #(.substring % 0 (.lastIndexOf % ".")) #(.getName ^File %))))]
    (md/create-ns-metadata! table)
    (log/infof "About to load the following segments for table %s : %s "
               table
               segment-ids)
    (let [active-segment (s/roll-new-segment! table (inc (first segment-ids)))
          read-segments (zipmap segment-ids (doall (map #(load-read-only-segment
                                                           table
                                                           %) segment-ids)))]
      ;TODO shut down existing stuff first or check?
      (md/set-active-segment-for-table! table active-segment)
      (md/set-old-segments-for-table! table read-segments))
    ))