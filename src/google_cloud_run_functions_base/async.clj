(ns google-cloud-run-functions-base.async
  (:require [clojure.core.async :as a :refer [<!! >!! <! >!]]))

(def channel-a (a/chan (a/dropping-buffer 5)))
(comment
  (do
    (>!! channel-a "hello 1!")
    (>!! channel-a "hello 2!")
    (>!! channel-a "hello 3!")
    (>!! channel-a "hello 4!")
    (>!! channel-a "hello 5!"))

  (>!! channel-a "hello 6!")

  (<!! channel-a)

  (a/close! channel-a)

  channel-a


  (a/go (>! channel-a "Test 1"))
  (<!! (a/go (>! channel-a "Test 2")))
  (a/go (>! channel-a "Test 3"))
  (a/go (>! channel-a "Test 4"))
  (a/go (>! channel-a "Test 5"))
  (a/go (>! channel-a "Test 6"))

  (<!! (a/go (<! channel-a)))


  (let [c (a/chan)]
    (a/go (>! c "hello"))
    (assert (= "hello" (<!! (a/go (<! c)))))
    (a/close! c)))