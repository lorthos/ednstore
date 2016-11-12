(ns ednstore.store.writer
  (:require
    [ednstore.store.segment]
    [ednstore.serialization.core :as ser]
    [ednstore.io.core :refer :all])
  (:import (ednstore.store.segment ActiveSegment)))

(defn write!
  "write to the active segment only, should not write to an inactive segment
  active segment looks like this:
  index last-offset read-chan write-chan

  1.update index
  2.update increment last offset
  3.write
  "
  [k v ^ActiveSegment segment]
  (let [key (ser/field->wire k)
        val (ser/field->wire v)
        barray (ser/create-append-log key val)
        append-offset-length (alength barray)]

    ;TODO should be atomic
    (swap! (:index segment) assoc k @(:last-offset segment))
    (swap! (:last-offset segment) + append-offset-length)
    (write (:wc segment) barray)
    )
  )

(defn delete!
  "write to log with the delete marker
  1.append to file
  2.update index
  3.append segment offset counter"
  [k ^ActiveSegment segment]
  (let [barray (ser/create-tombstone-log (ser/field->wire k))
        append-offset-length (alength barray)]
    ;TODO atomic
    (swap! (:index segment) assoc k @(:last-offset segment))
    (swap! (:last-offset segment) + append-offset-length)
    (write (:wc segment) barray)
    )
  )

