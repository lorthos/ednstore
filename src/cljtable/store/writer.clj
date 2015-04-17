(ns cljtable.store.writer
  (:require [nio.core :as nio]
            [clojure.java.io :as io]
            [cljtable.store.segment])
  (:import (java.nio.channels WritableByteChannel)
           (java.nio ByteBuffer)
           (java.io ByteArrayOutputStream)
           (cljtable.store.segment ActiveSegment))
  )

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

;(->> value
;     encode
;     append-to-log
;     get-end-offset
;     )


(defn get-log-line
  "format will be: LENGTH:KEY:LENGTH:VALUE"
  [#^bytes k #^bytes v]
  (let [out (ByteArrayOutputStream.)
        key-size (ByteBuffer/allocate 4)
        kl (alength k)
        vl (alength v)
        value-size (ByteBuffer/allocate 4)]
    (.write out (.array (.putInt key-size kl)))
    (.write out k)
    (.write out (.array (.putInt value-size vl)))
    (.write out v)
    (.toByteArray out)))

(defn put!
  "write to the active segment only, should not write to an inactive segment
  active segment looks like this:
  index last-offset read-chan write-chan
  "
  [^String k ^String v ^ActiveSegment segment]
  (let [key (.getBytes k "UTF-8")
        val (.getBytes v "UTF-8")
        barray (get-log-line key val)
        append-offset-length (alength barray)]
    ;update index
    ;update increment last offset
    ;write
    ;TODO atomic
    (swap! (:index segment) assoc k @(:last-offset segment))
    (swap! (:last-offset segment) + append-offset-length)
    (.write (:write-chan segment) (nio/byte-buffer barray))
    ;(.flush (:write-chan segment))
    )
  ;find the segments index
  ;segments current offset will be the index value
  ;append to file
  ;update index
  ;append segment offset counter
  )


(defn append! [segment])

