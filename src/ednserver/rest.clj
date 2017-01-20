(ns ednserver.rest
  (:require
    [compojure.core :refer [GET POST PUT DELETE ANY defroutes routes]]
    [compojure.handler :as handler]
    [clojure.tools.logging :as log]
    [ednstore.common :refer [initialize! stop! lookup delete! insert!]]
    [ednstore.store.merge.controller :as mcon])
  (:use org.httpkit.server)
  (:import (ednstore.core SimpleDiskStore)
           (java.util.concurrent Executors)
           (org.httpkit BytesInputStream)))

(def server-state (atom {}))

(defn slurp-body [req]
  (String. (.bytes ^BytesInputStream (:body req))))

;rest related
(defroutes health-routes
           (GET ["/ping"] req
             "pong"))

(defroutes storage-routes
           (GET "/lookup/:table/:record-key" [table record-key]
             (let [dbval (lookup (:store @server-state) table record-key)]
               (log/infof "looked up db value: %s for table %s and key %s" dbval table record-key)
               (str dbval)))

           (DELETE "/lookup/:table/:record-key" [table record-key]
             (str
               (delete! (:store @server-state) table record-key)))

           (POST "/lookup/:table/:record-key" [table record-key :as request]
             (let [post-body (slurp-body request)]
               (log/infof "http request: %s" request)
               (log/infof "inserting new value: %s to table %s and key: %s " post-body table record-key)
               {:body (str (insert! (:store @server-state) table record-key post-body))}))
           )

(def app
  (->
    (routes
      health-routes
      storage-routes)
    handler/api))

(defn start-server! [port cli-options]

  (let [exec-pool (Executors/newSingleThreadExecutor)
        merge-pool (mcon/make-merger-pool! (:merge-trigger-interval-sec cli-options))]
    (log/infof "Starting EdnStore for EdnServer...")
    (swap! server-state assoc :store (SimpleDiskStore. exec-pool merge-pool))
    (initialize! (:store @server-state) cli-options)
    )


  (log/infof "Starting Http server for EdnServer on port: %s" port)
  (let [http-server (run-server app {:port port})]
    (swap! server-state assoc :http-server http-server)))

(defn stop-server! []
  ;call handler to stop rest server
  ((:http-server @server-state))
  ;stop storage
  (stop! (:store @server-state))
  (reset! server-state (atom {}))
  )



