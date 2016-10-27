(ns cljtable.io.core
  "reads serialization format from underlying channel"
  (:require [cljtable.serialization.core :as ser])
  (:import (java.nio.channels SeekableByteChannel)
           (java.nio ByteBuffer)))


(defn read-byte-from-chan [^SeekableByteChannel chan]
  (let [buf (ByteBuffer/allocate 1)]
    (.read chan buf)
    (.flip buf)
    (.get buf)))

(defn read-int-from-chan [^SeekableByteChannel chan]
  (let [buf (ByteBuffer/allocate 4)]
    (.read chan buf)
    (.flip buf)
    (.getInt buf)))

(defn read-type-from-chan [^SeekableByteChannel chan length]
  (let [buf (ByteBuffer/allocate length)]
    (.read chan buf)
    (ser/wire->field (.array buf))))
