(ns google-cloud-run-functions-base.http-server
  (:require [clojure.core.async :as a :refer [<! >!]]
            [clojure.string :refer [split trim]])
  (:import (java.io DataInputStream ByteArrayOutputStream)
           (java.net ServerSocket)))

(def server-socket* (atom nil))
(def http-read-channel (a/chan (a/dropping-buffer 100))) ; TODO parametrize size of buffer
;(def http-requests-channel (a/chan (a/dropping-buffer 100))) ; TODO parametrize size of buffer

(defn open-server-socket
  "Opens a sever socket on the given port."
  [port]
  (when
   (or (nil? @server-socket*)
       (.isClosed ^ServerSocket @server-socket*))
    (let [server-socket (ServerSocket. port)]
      (println "Opened server socket on port" port)
      (reset! server-socket* server-socket)
      server-socket)))

(def line-parsers
  {:first_line
   (fn [line request]
     (let [[method path version] (split line #" ")]
       (assoc request
              :method method
              :path path
              :http-version version)))

   :headers
   (fn [line request]
     (let [[key value] (split line #": " 2)]
       (assoc-in request [:headers key] value)))})

(defn parse
  [line section request]
  ((line-parsers section) (trim line) request))

(defn read-line-from-stream
  "Reads a line from DataInputStream byte by byte until \\r\\n is found.
   Returns the line as a string without the \\r\\n."
  [^DataInputStream input-stream]
  (let [line-buffer (ByteArrayOutputStream.)]
    (loop [previous-byte nil]
      (let [current-byte (.read input-stream)]
        (cond
          (= current-byte -1) nil ; End of stream
          (and (= previous-byte 13) (= current-byte 10)) ; Found \r\n
          ;; The \r was never written to buffer, so just convert what's there
          (String. (.toByteArray line-buffer) "UTF-8")
          :else
          (do
            (when previous-byte (.write line-buffer (int previous-byte)))
            (recur current-byte)))))))

(defn dispatch-request [request client-socket]
  (let [out (.getOutputStream client-socket)
        response-body (str "Hello! You requested " (:path request) " with method " (:method request) "\n")
        response (str "HTTP/1.1 200 OK\r\n"
                      "Content-Type: text/plain\r\n"
                      "Content-Length: " (count response-body) "\r\n"
                      "Connection: close\r\n"
                      "\r\n"
                      response-body)]
    (println "dispatching request:" request)
    (.write out (.getBytes response))
    (.flush out)
    (println "Response sent to" (.getInetAddress client-socket))
    (.close out)
    (.close client-socket)))

(defn- empty-line-after-header? [section line]
  (and (= section :headers) (empty? line)))

(defn handle-request [input-stream]
  (try
    ;; Read headers line by line
    (loop [line (read-line-from-stream input-stream)
           line-index 0
           section :first_line
           request {}]
      (println "(" section "): " "'" line "' isNil?" (nil? line) "; type of section: " (type section))
      (cond
        (empty-line-after-header? section line)
        (let [content-length (or (some-> (get-in request [:headers "Content-Length"])
                                         Integer/parseInt)
                                 0)
              body-string (if (pos? content-length)
                            (let [buffer (byte-array content-length)]
                              (print "Reading " content-length " bytes of body")
                              (.readFully input-stream buffer)
                              (String. buffer "UTF-8"))
                            "")
              final-request (assoc request :body body-string)]
          (println "Request complete:" final-request)
          final-request)

        ;; End of stream
        (nil? line)
        (do
          (println "This should not happen!")
          request)

        ;; Continue reading headers
        :else
        (recur
         (read-line-from-stream input-stream)
         (inc line-index)
         (if (= section :first_line) :headers section)
         (parse line section request))))
    (catch Exception e
      (println "Error handling HTTP request:" e))))

(defn setup-request-attending-routine
  "Sets up routines that handle async events for HTTP."
  []
  (a/go
    (while true
      (let [client-socket (<! http-read-channel)
            input-stream (DataInputStream. (.getInputStream client-socket))]
        (println "*Handling HTTP request from" (some-> client-socket .getInetAddress .getHostAddress))
        (-> (handle-request input-stream)
            (dispatch-request client-socket))))))

(defn start-attending-http-requests
  "Starts a routine that attends incoming HTTP requests."
  [server-socket]
  (while true
    (try
      (println "Waiting for connections...")
      (let [client-socket (.accept server-socket)
            inetAddress (.getInetAddress client-socket)]
        (println "Accepted connection from: " (.getHostName inetAddress))

        (a/go (>! http-read-channel client-socket)))
      (catch Exception e
        (.printStackTrace e)))))

(defn get-port-number-from-environment
  "Gets the port number from the environment variable PORT, or returns 8083 as default."
  []
  (or (some-> (System/getenv "PORT")
              Integer/parseInt)
      8083))

(defn start
  "Main entry point."
  [& _]
  (try
    (setup-request-attending-routine)
    (->
     (get-port-number-from-environment)
     (open-server-socket)
     (start-attending-http-requests))
    (println "Server stopped.")
    (catch Exception e
      (.printStackTrace e))))
