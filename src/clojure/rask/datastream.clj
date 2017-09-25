(ns rask.datastream
  (:refer-clojure :exclude [filter map print reduce])
  (:require [rask.util :as util])
  (:import [org.apache.flink.api.common.functions FlatMapFunction ReduceFunction MapFunction FilterFunction FoldFunction]
           [org.apache.flink.util Collector]
           [org.apache.flink.streaming.api.datastream DataStream KeyedStream SingleOutputStreamOperator
                                                      DataStreamSink WindowedStream]
           [org.apache.flink.api.common.typeinfo TypeHint TypeInformation]
           [org.apache.flink.api.java.functions KeySelector]
           [rask.api RequiringFunction]))

(defn ^DataStream map
  "Takes one element and produces one element.

  Accepts a one arg function"
  [f ^DataStream stream]
  (let [p (proxy [RequiringFunction MapFunction] [f]
            (map [x]
              (f x)))]
    (.map stream p)))

(defn ^DataStream flat-map
  "Takes one element and produces zero, one, or more elements.

  Accepts a function f that should take one argument and return a sequence of results
  that would be added to collector."
  [f ^DataStream stream]
  (let [p (proxy [RequiringFunction FlatMapFunction] [f]
            (flatMap [x ^Collector acc]
              (doseq [y (f x)]
                (.collect acc y))))]
    (.flatMap stream p)))

(defn ^DataStream filter
  "Evaluates a predicate for each element and retains those for which the predicate returns true.

  Accepts a one arg function that would be coerced to boolean."
  [f ^DataStream stream]
  (let [p (proxy [RequiringFunction FilterFunction] [f]
            (filter [x]
              (boolean (f x))))]
    (.filter stream p)))

(defn ^DataStream reduce
  "For KeyedStream:
  A \"rolling\" reduce on a keyed data stream. Combines the current element with the last reduced value and
  emits the new value.

  For WindowedStream:
  Applies a functional reduce function to the window and returns the reduced value.

  Accepts a function f that takes two arguments. If val is supplied it would be used as an initial valiue."
  ([f ^DataStream stream]
   (let [p (proxy [RequiringFunction ReduceFunction] [f]
             (reduce [x y]
               (f x y)))]
     (cond
       (instance? KeyedStream stream) (.reduce ^KeyedStream stream p)
       (instance? WindowedStream stream) (.reduce ^WindowedStream stream p)
       :else (throw
               (IllegalArgumentException.
                 (format "Unsupported stream type: %s" (class stream)))))))
  ([f val ^DataStream stream]
   (let [p (proxy [RequiringFunction FoldFunction] [f]
             (fold [x y]
               (f x y)))]
     (cond
       (instance? KeyedStream stream) (.fold val ^KeyedStream stream p)
       (instance? WindowedStream stream) (.fold val ^WindowedStream stream p)
       :else (throw
               (IllegalArgumentException.
                 (format "Unsupported stream type: %s" (class stream))))))))

(defn ^KeyedStream by-key
  "Logically partitions a stream into disjoint partitions, each partition containing elements of the same key.

   Accepts fields which be can be a sequence of:
   * indexes
   * names of a public fields
   * getter methods with parentheses of the stream underlying type
   or a KeySelector and a stream."
  [key ^DataStream stream]
  (if (instance? KeySelector key)
    (.keyBy stream ^KeySelector key)
    (cond
      (sequential? key)
      (let [[first-key] key]
        (cond
          (number? first-key) (.keyBy stream ^ints (int-array key))
          (string? first-key) (.keyBy stream ^"[Ljava.lang.String;" (into-array String key))))

      (number? key)
      (.keyBy stream ^ints (int-array [key]))

      (string? key)
      (.keyBy stream ^"[Ljava.lang.String;" (into-array String [key]))

      :else
      (throw
        (IllegalArgumentException.
          (format "Unsupported key %s: " (class key)))))))

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
   (.timeWindow stream (util/time size)))
  ([size slide ^KeyedStream stream]
   (.timeWindow stream (util/time size) (util/time slide))))

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