(ns google-cloud-run-functions-base.core
  (:import (java.io BufferedReader InputStreamReader)
           (java.net ServerSocket)))

(def server-socket* (atom nil))

(defn open-socket
  "Opens a sever socket on the given port."
  [port]
  (if
    (or (nil? @server-socket*)
        (.isClosed ^ServerSocket @server-socket*))
    (let [server-socket (ServerSocket. port)]
      (println "Opened server socket on port" port )
      (reset! server-socket* server-socket)))
  (let [server-socket @server-socket*]
    (while true
      (do (println "Waiting for connection on port" port)
          (let [client-socket (.accept server-socket)
                inetAddress (.getInetAddress client-socket)]
            (println "Accepted connection from: " (.getHostName inetAddress))
            (try
              (with-open [in (-> client-socket
                                 .getInputStream
                                 InputStreamReader.
                                 BufferedReader.)]
                (loop [line (.readLine in)]
                  (if (and (not= line -1)
                             (not (nil? line)))
                    (do
                      (println (type line) (nil? line) "'" line "'")
                      (recur (.readLine in)))
                    (println "End of stream reached? '" line "'"))))
              (finally (println "\nFinished reading from socket.")
                       (.close client-socket))))))))

(defn -main
  "Main entry point."
  [& args]
  (let [port (or (some-> (System/getenv "PORT")
                         Integer/parseInt)
                 8083)]
    (open-socket port)))

(comment (-main 1))
