(ns rask.examples.streaming.windowed-word-count
  "Word count from string socket stream with time window."
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
   [nil "--window MILLISECOND" "Window length"
    :default 5000
    :parse-fn #(Long/parseLong %)]
   ["-h" "--help"]])

(deftype WordCount [word count])

(def custom-sum
  (u/fn [acc x]
    (u/tuple
      (u/nth acc 0)
      (+ (u/nth acc 1)
         (u/nth x 1)))))

(defn -main [& args]
  (let [{:keys [options summary]} (cli/parse-opts args cli-options)]
    (if (or (:help options) (not (:port options)))
      (binding [*out* *err*]
        (println "Usage:\n")
        (println summary))
      (let [{:keys [host port window]} options
            env (s/env)]
        (->>
          (s/stream env {:host host :port port})
          (s/mapcat
            (u/fn [x]
              (let [xs (-> x string/lower-case (string/split #"\W+"))]
                (map #(u/tuple % 1) xs)))
            (u/tuple-hint String Long))
          (s/group-by 0)
          (s/time-window window)
          (s/reduce custom-sum)
          s/print)
        (s/execute env "Clojure time windowed word count from socket text stream.")))))