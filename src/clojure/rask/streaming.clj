(ns rask.streaming
  (:refer-clojure :exclude [filter map max min print reduce])
  (:require [rask.util :as util])
  (:import [org.apache.flink.api.common.typeinfo TypeHint TypeInformation]
           [org.apache.flink.core.fs FileSystem$WriteMode]
           [org.apache.flink.streaming.api.datastream DataStream KeyedStream SingleOutputStreamOperator
                                                      DataStreamSink WindowedStream]
           [org.apache.flink.streaming.api.windowing.assigners WindowAssigner]
           [org.apache.flink.streaming.api.functions AssignerWithPeriodicWatermarks AssignerWithPunctuatedWatermarks]
           [org.apache.flink.api.common.functions FlatMapFunction MapFunction FilterFunction ReduceFunction FoldFunction JoinFunction]
           [org.apache.flink.util Collector]
           [org.apache.flink.streaming.api.functions.sink SinkFunction]
           [org.apache.flink.api.java.functions KeySelector]
           [org.apache.flink.api.java.typeutils ResultTypeQueryable]))

(defn ^DataStream map
  "Takes one element and produces one element.

  Accepts a one arg function"
  [f ^DataStream stream]
  (let [f (reify MapFunction
            (map [_ x]
              (f x)))]
    (.map stream f)))

(defn ^DataStream flat-map
  "Takes one element and produces zero, one, or more elements.

  Accepts a function f that should take one argument and return a sequence of results
  that would be added to collector."
  [f ^DataStream stream]
  (let [f (reify FlatMapFunction
            (^void flatMap [_ x ^Collector acc]
              (doseq [y (f x)]
                (.collect acc y))))]
    (.flatMap stream f)))

(defn ^DataStream filter
  "Evaluates a predicate for each element and retains those for which the predicate returns true.

  Accepts a one arg function that would be coerced to boolean."
  [f ^DataStream stream]
  (let [f (reify FilterFunction
            (filter [_ x]
              (boolean (f x))))]
    (.filter stream f)))

(defn ^DataStream reduce
  "For KeyedStream:
  A \"rolling\" reduce on a keyed data stream. Combines the current element with the last reduced value and
  emits the new value.

  For WindowedStream:
  Applies a functional reduce function to the window and returns the reduced value.

  Accepts a function f that takes two arguments - accumulator and value.

  If val is supplied it would be used as an initial valiue."
  ([f ^DataStream stream]
   (let [f (reify ReduceFunction
             (reduce [_ acc x]
               (f acc x)))]
     (cond
       (instance? KeyedStream stream) (.reduce ^KeyedStream stream f)
       (instance? WindowedStream stream) (.reduce ^WindowedStream stream f)
       :else (throw
               (IllegalArgumentException.
                 (format "Unsupported stream type: %s" (class stream)))))))
  ([f val ^DataStream stream]
   (let [f (reify FoldFunction
             (fold [_ acc x]
               (f acc x)))]
     (cond
       (instance? KeyedStream stream) (.fold val ^KeyedStream stream f)
       (instance? WindowedStream stream) (.fold val ^WindowedStream stream f)
       :else (throw
               (IllegalArgumentException.
                 (format "Unsupported stream type: %s" (class stream))))))))

(defn join
  [^DataStream stream-1 ^DataStream stream-2 left-key right-key window join-fn]
  (let [k1 (reify
             KeySelector
             (getKey [_ x]
               (left-key x))
             ResultTypeQueryable
             (getProducedType [_] (.getTypeInfo ^TypeHint (util/type-hint Object))))
        k2 (reify
             KeySelector
             (getKey [_ x]
               (right-key x))
             ResultTypeQueryable
             (getProducedType [_] (.getTypeInfo ^TypeHint (util/type-hint Object))))
        f (reify
            JoinFunction
            (join [_ x y]
              (join-fn x y))
            ResultTypeQueryable
            (getProducedType [_] (.getTypeInfo ^TypeHint (util/type-hint Object))))]
    (.apply
      (.window
        (.equalTo
          (.where
            (.join stream-1 stream-2) ^KeyedStream k1)
          ^KeySelector k2)
        window)
      ^JoinFunction f)))

(defn ^KeyedStream key-by
  "Logically partitions a stream into disjoint partitions, each partition containing elements of the same key.

   Accepts a key which be can be:

   a sequence of:
   * indexes
   * names of a public fields
   * getter methods with parentheses of the stream underlying type

   or
   * a one arg function that returns a key"
  [key ^DataStream stream]
  (cond
    (fn? key)
    (let [f (reify KeySelector
              (getKey [_ x]
                (key x)))]
      ;; TODO correct add type-hint
      (KeyedStream. stream f (.getTypeInfo ^TypeHint (util/type-hint Object))))

    ;; use interop instead
    (instance? KeySelector key)
    (.keyBy stream ^KeySelector key)

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
        (format "Unsupported key %s: " (class key))))))

(defn ^SingleOutputStreamOperator sum
  "Applies an aggregation that gives a rolling sum of the data stream at the given position grouped by the given key.
   An independent aggregate is kept per key.

   key can be a field index or name
   stream has to be a KeyedStream or a WindowedStream
   "
  [key ^DataStream stream]
  (cond
    (instance? KeyedStream stream)
    (cond
      (number? key) (.sum ^KeyedStream stream (int key))
      (string? key) (.sum ^KeyedStream stream ^String key))

    (instance? WindowedStream stream)
    (cond
      (number? key) (.sum ^WindowedStream stream (int key))
      (string? key) (.sum ^WindowedStream stream ^String key))))

(defn ^SingleOutputStreamOperator min
  "Applies an aggregation that gives the current minimum of the data stream at the given field expression by the
  given key. An independent aggregate is kept per key."
  [key ^DataStream stream]
  (cond
    (instance? KeyedStream stream)
    (cond
      (number? key) (.min ^KeyedStream stream (int key))
      (string? key) (.min ^KeyedStream stream ^String key))

    (instance? WindowedStream stream)
    (cond
      (number? key) (.min ^WindowedStream stream (int key))
      (string? key) (.min ^WindowedStream stream ^String key))))

(defn ^SingleOutputStreamOperator min-by
  "Applies an aggregation that gives the current element with the minimum value at the given position by the
  given key. An independent aggregate is kept per key.

  If more elements have the minimum value at the given position, the operator returns the first one by default
  unless first? is set to false."
  ([key ^DataStream stream]
   (min-by key true stream))
  ([key first? ^DataStream stream]
   (cond
     (instance? KeyedStream stream)
     (cond
       (number? key) (.minBy ^KeyedStream stream (int key) ^boolean first?)
       (string? key) (.minBy ^KeyedStream stream ^String key ^boolean first?))

     (instance? WindowedStream stream)
     (cond
       (number? key) (.minBy ^WindowedStream stream (int key) ^boolean first?)
       (string? key) (.minBy ^WindowedStream stream ^String key ^boolean first?)))))

(defn ^SingleOutputStreamOperator max
  "Applies an aggregation that gives the current maximum of the data stream at the given field expression by the
  given key. An independent aggregate is kept per key."
  [key ^DataStream stream]
  (cond
    (instance? KeyedStream stream)
    (cond
      (number? key) (.max ^KeyedStream stream (int key))
      (string? key) (.max ^KeyedStream stream ^String key))

    (instance? WindowedStream stream)
    (cond
      (number? key) (.max ^WindowedStream stream (int key))
      (string? key) (.max ^WindowedStream stream ^String key))))

(defn ^SingleOutputStreamOperator max-by
  "Applies an aggregation that gives the current element with the maximum value at the given position by the
  given key. An independent aggregate is kept per key.

  If more elements have the minimum value at the given position, the operator returns the first one by default
  unless first? is set to false."
  ([key ^DataStream stream]
   (max-by key true stream))
  ([key first? ^DataStream stream]
   (cond
     (instance? KeyedStream stream)
     (cond
       (number? key) (.maxBy ^KeyedStream stream (int key) ^boolean first?)
       (string? key) (.maxBy ^KeyedStream stream ^String key ^boolean first?))

     (instance? WindowedStream stream)
     (cond
       (number? key) (.maxBy ^WindowedStream stream (int key) ^boolean first?)
       (string? key) (.maxBy ^WindowedStream stream ^String key ^boolean first?)))))

(defn ^SingleOutputStreamOperator timestamps-and-watermarks
  "Assigns timestamps to the elements in the data stream and, depending on the assigner:

  * periodically creates watermarks to signal event time progress
  * creates watermarks to signal event time progress based on the elements themselves"
  ([assigner ^DataStream stream]
   (cond
     (instance? AssignerWithPeriodicWatermarks assigner)
     (.assignTimestampsAndWatermarks stream ^AssignerWithPeriodicWatermarks assigner)

     (instance? AssignerWithPunctuatedWatermarks assigner)
     (.assignTimestampsAndWatermarks stream ^AssignerWithPunctuatedWatermarks assigner)

     :else
     (throw (IllegalArgumentException. (str "Unsupported assigner: " assigner))))))

(defn ^WindowedStream window
  "Windows this data stream to a WindowedStream, which evaluates windows over a key grouped stream.

  Elements are put into windows by a assigner. The grouping of elements is done both by key and by window."
  ([^WindowAssigner assigner ^KeyedStream stream]
   (.window stream assigner)))

(defn ^WindowedStream time-window
  "Windows this KeyedStream into tumbling time windows.

  size and slide should be either number of milliseconds, or instances of
  org.apache.flink.streaming.api.windowing.time.Time"
  ([size ^KeyedStream stream]
   (.timeWindow stream (util/time size)))
  ([size slide ^KeyedStream stream]
   (.timeWindow stream (util/time size) (util/time slide))))

(defn ^WindowedStream count-window
  "Windows this KeyedStream into tumbling count windows or into sliding count if slide is provided.

  size - the size of the window
  slide - interval in number of elements"
  ([size ^KeyedStream stream]
   (.countWindow stream size))
  ([size slide ^KeyedStream stream]
   (.countWindow stream size slide)))

(defn ^WindowedStream evictor
  "Sets the Evictor that should be used to evict elements from a window before emission.

  evictor should be a instance of org.apache.flink.streaming.api.windowing.evictors.Evictor."
  [evictor ^WindowedStream stream]
  (.evictor stream evictor))

(defn trigger
  "Sets the Trigger that should be used to trigger window emission.

  trigger should be an instance of org.apache.flink.streaming.api.windowing.triggers.Trigger."
  [trigger ^WindowedStream stream]
  (.trigger stream trigger))

(defn ^SingleOutputStreamOperator returns
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

(defn ^DataStreamSink parallelism
  "Sets the parallelism for this sink.

  n must be higher than zero."
  [n ^DataStreamSink stream]
  (.setParallelism stream n))

;; --------------------------------------------------------------------------------------------------------
;; sinks
;; --------------------------------------------------------------------------------------------------------

(defn ^DataStreamSink add-sink
  "Adds the given sink to this DataStream. Only streams with sinks added will be executed once the
  (rask.environment/execute env) function is called.

  Accepts a SinkFunction implementation or a one-arg function that would be wrapped in SinkFunction."
  [f ^DataStream stream]
  (if (instance? SinkFunction f)
    (.addSink stream f)
    (let [f (reify SinkFunction
              (invoke [_ x]
                (f x)))]
      (.addSink stream f))))

(defn ^DataStreamSink print
  "Writes a DataStream to the standard output stream (stdout)."
  [^DataStream stream]
  (.print stream))

(defn ^DataStreamSink print-to-err
  "Writes a DataStream to the standard output stream (stderr). "
  [^DataStream stream]
  (.printToErr stream))

(defn ^DataStreamSink write-as-text
  "Writes a DataStream to the file specified by path in text format."
  ([path ^DataStream stream]
   (.writeAsText stream path))
  ([path mode ^DataStream stream]
   (if-let [mode
            (cond
              (= mode :no-overwrite) FileSystem$WriteMode/NO_OVERWRITE
              (= mode :overwrite) FileSystem$WriteMode/OVERWRITE
              :else nil)]
     (.writeAsText stream path mode)
     (write-as-text path stream))))