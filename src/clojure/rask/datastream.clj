(ns rask.datastream
  (:refer-clojure :exclude [print reduce])
  (:require [rask.data :as data])
  (:import [org.apache.flink.api.common.functions FlatMapFunction ReduceFunction]
           [org.apache.flink.util Collector]
           [org.apache.flink.streaming.api.datastream DataStream KeyedStream SingleOutputStreamOperator
                                                      DataStreamSink WindowedStream]
           [org.apache.flink.api.common.typeinfo TypeHint TypeInformation]
           [org.apache.flink.api.java.functions KeySelector]
           [rask.api RequiringFunction]))

(defn flat-map
  "Accepts a function f that should take one argument and return a sequence of results
  that would be added to collector."
  [f ^DataStream stream]
  (let [p (proxy [RequiringFunction FlatMapFunction] [f]
            (flatMap [x ^Collector acc]
              (doseq [y (f x)]
                (.collect acc y))))]
    (.flatMap stream p)))

(defn reduce
  "Accepts a function f that takes two arguments."
  [f ^DataStream stream]
  (let [p (proxy [RequiringFunction ReduceFunction] [f]
            (reduce [x y]
              (f x y)))]
    ;; a bit ugly
    (cond
      (instance? KeyedStream stream) (.reduce ^KeyedStream stream p)
      (instance? WindowedStream stream) (.reduce ^WindowedStream stream p)
      :else (.reduce stream p))))

(defn by-key
  "Partitions the operator state of a stream by the given key positions.

   Accepts fields which be can be a sequence of:
   * indexes
   * names of a public fields
   * getter methods with parentheses of the stream underlying type
   or a KeySelector and a stream"
  [fields-or-key-selector ^DataStream stream]
  (if (or (seq? fields-or-key-selector)
          (vector? fields-or-key-selector))
    (let [[field :as fields] fields-or-key-selector]
      (cond
        (number? field) (.keyBy stream ^ints (int-array fields))
        (string? field) (.keyBy stream ^"[Ljava.lang.String;" (into-array String fields))))
    (.keyBy stream ^KeySelector fields-or-key-selector)))

(defn sum
  "Applies an aggregation that gives a rolling sum of the data stream at the given position grouped by the given key.
   An independent aggregate is kept per key.

   key can be a field index or name
   "
  [key ^KeyedStream stream]
  (cond
    (number? key) (.sum stream (int key))
    (string? key) (.sum stream ^String key)))

(defn time-window
  "Windows this KeyedStream into tumbling time windows.

  size and slide should be either number of milliseconds, or instances of
  org.apache.flink.streaming.api.windowing.time.Time"
  ([size ^KeyedStream stream]
   (.timeWindow stream (data/time size)))
  ([size slide ^KeyedStream stream]
   (.timeWindow stream (data/time size) (data/time slide))))

(defn returns
  "Adds a type information hint about the return type of this operator.
   Use this when Flink cannot determine automatically what the produced type of a function is.

   Classes can be used as type hints for non-generic types (classes without generic parameters)
   For generic types like for example Tuples use the TypeHint method.
   Also accepts TypeInformation as hint."
  [class-or-type ^SingleOutputStreamOperator stream]
  (cond
    (instance? Class class-or-type)           (.returns stream ^Class class-or-type)
    (instance? TypeHint class-or-type)        (.returns stream ^TypeHint class-or-type)
    (instance? TypeInformation class-or-type) (.returns stream ^TypeInformation class-or-type)))

(defn parallelism
  "Sets the parallelism for this sink.

  n must be higher than zero."
  [n ^DataStreamSink stream]
  (.setParallelism stream n))

(defn print
  "Writes a DataStream to the standard output stream (stdout)."
  [stream]
  (.print stream))

(defn print-to-err
  "Writes a DataStream to the standard output stream (stderr). "
  [stream]
  (.printToErr stream))