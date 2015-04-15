(ns cljtable.store.append
  (:import (java.nio.channels WritableByteChannel)
           (java.nio ByteBuffer))
  (:require [nio.core :as nio]
            [clojure.java.io :as io]))

(def channels (atom {}))
(def channel-offsets (atom {:channel1 0}))


(def chan (nio/writable-channel (io/file "test.tbl")))

(defn append-buffer!
  "appends the message to the end of the channel and returns the offset"
  [^WritableByteChannel chan, ^ByteBuffer message]
  (.write chan message)
  )


(defn prepare-string [strdata]
  (let [buflen (count strdata)
        bb (ByteBuffer/allocate buflen)]
    (doto bb
      (.put (.getBytes strdata))
      (.flip))
    bb))

(defn append-string! [^String message]
  (let [written (append-buffer! chan (prepare-string message))]
    (swap! channel-offsets assoc :channel1 (+ written (:channel1 @channel-offsets)))))



(defn create-file-channel! [^String path]
  (nio/writable-channel (io/file path)))

(swap! channels assoc :channel1 (create-file-channel! "test.log"))



@channels
@channel-offsets

(append-string! "asdQWE")
(append-string! "1234567890\n")

(defn close-channels! [chan-atom]
  (dorun (map #(.close ^WritableByteChannel %) (vals @chan-atom))))

(close-channels! channels)

