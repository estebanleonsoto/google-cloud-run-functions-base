(ns google-cloud-run-functions-base.core
  (:require [clojure.core.async :as a :refer [<!! >!! <! >!]]
            [clojure.string :refer [split trim]])
  (:import (java.io BufferedReader InputStreamReader InputStream)
           (java.net ServerSocket)))

(def server-socket* (atom nil))
(def http-read-channel (a/chan (a/dropping-buffer 100)))    ; TODO parametrize size of buffer
(def http-requests-channel (a/chan (a/dropping-buffer 100))) ; TODO parametrize size of buffer

(defn open-socket
  "Opens a sever socket on the given port."
  [port]
  (when
    (or (nil? @server-socket*)
        (.isClosed ^ServerSocket @server-socket*))
    (let [server-socket (ServerSocket. port)]
      (println "Opened server socket on port" port)
      (reset! server-socket* server-socket)))
  (let [server-socket @server-socket*]
    (while true
      (try
        (println "Waiting for connection on port" port)
        (let [client-socket (.accept server-socket)
              inetAddress (.getInetAddress client-socket)]
          (println "Accepted connection from: " (.getHostName inetAddress))

          (a/go (>! http-read-channel client-socket)))
        (catch Exception e
          (.printStackTrace e))))))

(def line-parsers
  {
   :first_line
   (fn [line request]
     (let [[method path version] (split line #" ")]
       (assoc request
         :method method
         :path path
         :http-version version)))

   :headers
   (fn [line request]
     (let [[key value] (split line #": " 2)]
       (assoc-in request [:headers key] value)))

   :done
   (fn [_ request]
     request)
   })

(defn parse
  [line section request]
  ((line-parsers section) (trim line) request))

(defn dispatch-request [request-data]
  (let [{:keys [request client-socket body-stream]} request-data
        out (.getOutputStream client-socket)
        ;; Example: Read body from input stream if needed
        ;; You can read the body-stream as needed here
        response-body (str "Hello! You requested " (:path request) " with method " (:method request) "\n")
        response (str "HTTP/1.1 200 OK\r\n"
                      "Content-Type: text/plain\r\n"
                      "Content-Length: " (count response-body) "\r\n"
                      "Connection: close\r\n"
                      "\r\n"
                      response-body)]
    (println "dispatching request:" request)
    (println "body-stream available:" body-stream)
    (.write out (.getBytes response))
    (.flush out)
    (println "Response sent to" (.getInetAddress client-socket))
    (.close out)
    (.close client-socket)))

(defn get-ready-for-http
  "Sets up routines that handle async events for HTTP."
  []
  (a/go
    (while true
      (let [client-socket (<! http-read-channel)
            input-stream (.getInputStream client-socket)
            in (BufferedReader. (InputStreamReader. input-stream))
            out (.getOutputStream client-socket)]
        (try
          (println "Handling HTTP request from" (-> client-socket .getInetAddress .getHostAddress))
          ;; Parse headers only, then pass the input stream for body reading
          (let [parsed-request
                (loop [line (.readLine in)
                       section :first_line
                       request {}]
                  (println "(" section "): " "'" line "'")
                  (cond
                    ;; After headers, we hit an empty line - stop parsing and return
                    (and (= section :headers) (empty? line))
                    (do
                      (println "Headers parsed, body stream ready")
                      request)

                    ;; End of stream
                    (nil? line)
                    (do
                      (println "End of stream")
                      request)

                    ;; Continue parsing headers
                    :else
                    (recur
                      (.readLine in)
                      (cond
                        (= section :first_line) :headers
                        :else section)
                      (parse line section request))))]
            ;; Dispatch with the input stream available for reading the body
            (dispatch-request {
                               :request       parsed-request
                               :client-socket client-socket
                               :body-stream   input-stream
                               }))
          (catch Exception e
            (println "Error handling HTTP request:" e)
            (.printStackTrace e))
          (finally
            (.close in)
            (.close client-socket)))))))


(defn -main
  "Main entry point."
  [& args]
  (try
    (let [port (or (some-> (System/getenv "PORT")
                           Integer/parseInt)
                   8083)]
      (get-ready-for-http)
      (open-socket port))
      (println "Server stopped.")
    (catch Exception e
      (.printStackTrace e))))

;(-main 1)
