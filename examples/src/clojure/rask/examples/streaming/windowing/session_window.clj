(ns rask.examples.streaming.windowing.session-window
  "Session windowing that keys events by id and groups and counts them in
   session with gaps of 3 milliseconds."
  (:require [clojure.tools.cli :as cli]
            [rask.streaming :as s]
            [rask.util :as u])
  (:gen-class))

(def cli-options
  [[nil "--output PATH" "Output path"]
   ["-h" "--help"]])

(def input
  (mapv u/into-tuple
    [["a" 1 1]
     ["b" 1 1]
     ["b" 3 1]
     ["b" 5 1]
     ["c" 6 1]
     ["a" 10 1]
     ["c" 11 1]]))

(def generator
  (u/source
    (u/fn [ctx _]
      (doseq [x input]
        (u/collect-with-timestamp ctx x (u/nth x 1))
        (u/emit-watermark ctx (dec (u/nth x 1))))
      (u/emit-watermark ctx Long/MAX_VALUE))))

(defn -main [& args]
  (let [{:keys [options]} (cli/parse-opts args cli-options)
        {:keys [output]} options
        env (s/env {:global-job-params args
                    :time-characteristic :event
                    :parallelism 1})
        stream
        (->> (s/stream env {:source generator :type (u/tuple-hint String Long Long)})
          (s/group-by 0)
          (s/window (s/time-window-assigner :with-gap 3))
          (s/sum 2))]

    (if output
      (s/to-file output stream)
      (s/print stream))

    (s/execute env "Clojure session windowing.")))