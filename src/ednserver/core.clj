(ns ednserver.core
  (:require
    [clojure.tools.logging :as log]
    [clojure.tools.cli :as cli]
    [ednserver.rest :as rest]))

(def cli-options
  [
   [nil "--path-to-config PATH_TO_CONFIG" "path to the config file"
    :default "~/ednstore/config.edn"]
   ])

(defn -main
  "Entry Point for the EdnServer process"
  [& args]
  (log/infof "Starting Appmaster: " args)
  (let [{:keys [options]} (cli/parse-opts args cli-options)
        path-to-config (:path-to-config options)
        parsed-config (->> path-to-config
                           slurp
                           read-string)
        ]

    (rest/start-server! (-> parsed-config
                            :ednserver
                            :rest-port) parsed-config)

    (.addShutdownHook (Runtime/getRuntime) (Thread. #(rest/stop-server!)))
    )
  )
