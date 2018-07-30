(ns rask.examples.streaming.word-count
  "Word count from string socket stream."
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [rask.streaming :as s]
            [rask.util :as u])
  (:gen-class))

(def cli-options
  [[nil "--host HOST" "Hostname"
    :default "localhost"]
   [nil "--port PORT" "Port"
    :parse-fn #(Integer/parseInt %)]
   ["-h" "--help"]])

(defn -main [& args]
  (let [{:keys [options summary]} (cli/parse-opts args cli-options)]
    (if (or (:help options) (not (:port options)))
      (binding [*out* *err*]
        (println "Usage:\n")
        (println summary))
      (let [{:keys [host port]} options
            env (s/env)]
        (->>
          (s/stream env {:host host :port port})
          (s/mapcat (fn [x] (-> x string/lower-case (string/split #"\W+"))))
          (s/map (u/fn [x] (u/tuple x (int 1))) (u/tuple-hint String Integer))
          (s/key-by 0)
          (s/sum 1)
          s/print)
        (s/execute env "Clojure word count from socket text stream.")))))