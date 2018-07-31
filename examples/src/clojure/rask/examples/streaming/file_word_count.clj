(ns rask.examples.streaming.file-word-count
  "Word count with file input/output with counting window."
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [rask.examples.samples :as samples]
            [rask.streaming :as s]
            [rask.util :as u]))

(def cli-options
   [nil "--input PATH" "Input path"]
   [nil "--output PATH" "Output path"]
   [nil "--size N" "Window length"
    :default 10
    :parse-fn #(Long/parseLong %)]
   [nil "--slide N" "Window slide"
    :default 5
    :parse-fn #(Long/parseLong %)]
   ["-h" "--help"])

(defn -main [& args]
  (let [{:keys [options]} (cli/parse-opts args cli-options)
        {:keys [input output size slide]} options
        env (s/env {:global-job-params options})]
    (when-not input
      (println "Executing with default input data.")
      (println "Use --input to specify input file."))
    (when-not output
      (println "Printing result to stdout. Use --output to specify output path."))

    (let [stream
          (->>
            (if input
              (s/stream env {:file input})
              (s/to-stream env samples/words))
            (s/mapcat
              (u/fn [x]
                (let [xs (-> x string/lower-case (string/split #"\W+"))]
                  (map #(u/tuple % 1) xs))))
            (s/group-by 0)
            (s/count-window size slide)
            (s/sum 1))]
      (if output
        (s/to-file output stream)
        (s/print stream))

      (s/execute env "Clojure counting windowed word count."))))