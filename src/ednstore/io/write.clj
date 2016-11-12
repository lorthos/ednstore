(ns ednstore.io.write
  (:require [nio.core :as nio]
            [ednstore.serialization.core :as ser])
  (:import (java.nio.channels WritableByteChannel)))


(defprotocol WriteChannel
  (write!! [this barray])
  (write-pair!! [this k v])
  (delete-key!! [this k])
  (close!! [this]))

(deftype NIOWriteChannel [^WritableByteChannel chan]
  WriteChannel
  (write!! [this barray]
    (.write chan (nio/byte-buffer barray)))

  (write-pair!! [this k v]
    (let [key (ser/field->wire k)
          val (ser/field->wire v)
          barray (ser/create-append-log key val)
          append-offset-length (alength barray)]
      (.write chan (nio/byte-buffer barray))
      append-offset-length))

  (delete-key!! [this k]
    (let [barray (ser/create-tombstone-log (ser/field->wire k))
          append-offset-length (alength barray)]
      (.write chan (nio/byte-buffer barray))
      append-offset-length))

  (close!! [this]
    (.close chan)))

(defn make-write-channel! [file]
  (NIOWriteChannel. (nio/writable-channel file)))
