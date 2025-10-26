(ns google-cloud-run-functions-base.core
  (:require [google-cloud-run-functions-base.http-server :as server]))

(defn -main [& args]
  (try
    (server/start)
    (catch Exception e
      (println "Error running server:" (.getMessage e))
      (.printStackTrace e))
    (finally
      (println "Server stopped."))))