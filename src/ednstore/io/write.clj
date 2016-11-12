(ns ednstore.io.write
  (:require [nio.core :as nio])
  (:import (java.nio.channels WritableByteChannel)))


(defprotocol WriteChannel
  (write!! [this barray])
  (close!! [this]))

(deftype NIOWriteChannel [^WritableByteChannel chan]
  WriteChannel
  (write!! [this barray]
    (.write chan (nio/byte-buffer barray)))
  (close!! [this]
    (.close chan)))

(defn make-write-channel! [file]
  (NIOWriteChannel. (nio/writable-channel file)))
