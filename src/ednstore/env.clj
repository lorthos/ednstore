(ns ednstore.env
  "Application properties"
  (:import (java.io PushbackReader))
  (:require [clojure.java.io :as io]))

(defn load-props [filename]
  (with-open [r (io/reader filename)]
    (binding [*read-eval* false]
      (read (PushbackReader. r)))))

(def config-path (or
                   (System/getProperty "ednstore.config.path")
                   "./resources/config.edn"))

(def ^:dynamic props (load-props (-> config-path io/file)))
