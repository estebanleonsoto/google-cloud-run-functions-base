(ns google-cloud-run-functions-base.core
  (:require [clojure.core.async :as a :refer [<!! >!! <! >!]]
            [clojure.string :refer [split trim]])
  (:import (java.io BufferedReader InputStreamReader)
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

   :body
   (fn [line request]
     (update request :body (fnil str "") (str line "\n")))

   :done
   (fn [_ request]
     request)
   })

(defn parse
  [line section request]
  ((line-parsers section) (trim line) request))

(defn dispatch-request [request-data]
  (let [{:keys [request client-socket]} request-data
        out (.getOutputStream client-socket)
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

(defn get-ready-for-http
  "Sets up routines that handle async events for HTTP."
  []
  (a/go
    (while true
      (let [client-socket (<! http-read-channel)
            in (BufferedReader. (InputStreamReader. (.getInputStream client-socket)))
            out (.getOutputStream client-socket)]
        (try
          (println "Handling HTTP request from" (-> client-socket .getInetAddress .getHostAddress))
          ;(dispatch-request
          (loop [line (.readLine in)
                 line-index 0
                 section :first_line
                 request {}]
            (println "(" section "): " "'" line "' isNil?" (nil? line))
            (if
              (= line-index 50)
              ;(or (= section :done)
              ;      (nil? line))
              (do
                (println "Se jue!")
                {
                 :request       request
                 :client-socket client-socket
                 })
              (recur
                (.readLine in)
                (inc line-index)
                (cond
                  (= section :first_line) :headers
                  (and (= section :headers) (empty? line)) :body
                  (nil? line) :done
                  :else section)
                ;(parse line section request)
                request
                )))
          ;)
          (catch Exception e
            (println "Error handling HTTP request:" e))
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
