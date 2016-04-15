(ns cljtable.common)


(defprotocol IKVStorage
  (initialize! [this config])
  (stop! [this])

  (insert! [this k v])
  (delete! [this k])
  (lookup [this k]))