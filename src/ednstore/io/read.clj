(ns ednstore.io.read
  "reads serialization format from underlying channel"
  (:require [ednstore.serialization.core :as ser]
            [nio.core :as nio])
  (:import (java.nio.channels SeekableByteChannel)
           (java.nio ByteBuffer)))


(defprotocol ReadChannel
  (position!! [this offset])
  (read-byte!! [this])
  (read-int!! [this])
  (read-wire-format!! [this length])
  (size!! [this])
  (close-read!! [this]))


(deftype NIOReadChannel [^SeekableByteChannel chan]
  ReadChannel
  (position!! [this offset]
    (.position chan offset))
  (read-byte!! [this]
    (let [buf (ByteBuffer/allocate 1)]
      (.read chan buf)
      (.flip buf)
      (.get buf)))
  (read-int!! [this]
    (let [buf (ByteBuffer/allocate 4)]
      (.read chan buf)
      (.flip buf)
      (.getInt buf)))
  (read-wire-format!! [this length]
    (let [buf (ByteBuffer/allocate length)]
      (.read chan buf)
      (ser/wire->field (.array buf))))
  (size!! [this]
    (.size chan))
  (close-read!! [this]
    (.close chan)))


(defn make-read-channel! [file]
  (NIOReadChannel. (nio/readable-channel file)))