(ns cljtable.io.core
  "reads serialization format from underlyting channel"
  (:require [cljtable.serialization.core :as ser]
            [cljtable.store.common :as c])
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

(defn read-nippy-from-chan [^SeekableByteChannel chan length]
  (let [buf (ByteBuffer/allocate length)]
    (.read chan buf)
    (ser/wire->field (.array buf))))
