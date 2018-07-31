(ns rask.streaming
  (:refer-clojure :exclude [group-by map mapcat print reduce])
  (:require [rask.util :as u])
  (:import [java.util Collection]
           [org.apache.flink.api.common.functions FlatMapFunction MapFunction ReduceFunction]
           [org.apache.flink.api.common.typeinfo TypeHint TypeInformation]
           [org.apache.flink.api.java.functions KeySelector]
           [org.apache.flink.api.java.utils ParameterTool]
           [org.apache.flink.core.fs FileSystem$WriteMode]
           [org.apache.flink.streaming.api.datastream DataStream DataStreamSink KeyedStream SingleOutputStreamOperator WindowedStream]
           [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.streaming.api.windowing.time Time]
           [org.apache.flink.util Collector]
           [org.apache.flink.streaming.api.functions.source SourceFunction]
           [org.apache.flink.streaming.api.windowing.assigners WindowAssigner EventTimeSessionWindows]
           [org.apache.flink.streaming.api TimeCharacteristic]))

(defn- ^TypeInformation coerce-type-information [x]
  (if (instance? TypeInformation x) x (TypeInformation/of ^Class x)))

(defn- coerce-time [x]
  (if (instance? Time x) x (Time/milliseconds x)))

(defn- coerce-time-event
  [event]
  (or ({:event      TimeCharacteristic/EventTime
        :ingestion  TimeCharacteristic/IngestionTime
        :processing TimeCharacteristic/ProcessingTime} event)
      event))

;; todo add remote and configuration options
(defn- get-or-create-env [options]
  (cond
    (:local? options)
    (StreamExecutionEnvironment/createLocalEnvironment)
    :else
    (StreamExecutionEnvironment/getExecutionEnvironment)))

(defn env
  ([]
   (env nil))
  ([options]
   (let [env (get-or-create-env options)]
     (when (:global-job-params options)
       (.setGlobalJobParameters
         (.getConfig env)
         (ParameterTool/fromArgs (into-array String (:global-job-params options)))))
     (when (:time-characteristic options)
       (.setStreamTimeCharacteristic env (coerce-time-event (:time-characteristic options))))
     (when (:parallelism options)
       (.setParallelism env (:parallelism options)))
     env)))

(defn execute
  [env & [job-name]]
  (.execute env (or job-name (str "rask-streaming-job-" (name (gensym ""))))))

(defn ^SingleOutputStreamOperator returns
  [class-or-type ^SingleOutputStreamOperator stream]
  (cond
    (instance? TypeInformation class-or-type) (.returns stream ^TypeInformation class-or-type)
    (instance? TypeHint class-or-type)        (.returns stream ^TypeHint class-or-type)
    (instance? Class class-or-type)           (.returns stream ^Class class-or-type)))

(defn ^SingleOutputStreamOperator map
  ([f ^DataStream stream]
   (map f Object stream))
  ([f return-hint ^DataStream stream]
   (let [f' (reify MapFunction
              (map [_ x]
                (f x)))]
     (returns return-hint (.map stream f')))))

(defn ^SingleOutputStreamOperator mapcat
  ([f ^DataStream stream]
   (mapcat f Object stream))
  ([f return-hint ^DataStream stream]
   (let [f' (reify FlatMapFunction
               (^void flatMap [_ x ^Collector acc]
                  (doseq [y (f x)]
                    (.collect acc y))))]
     (returns return-hint (.flatMap stream f')))))

(defn ^SingleOutputStreamOperator reduce [f ^DataStream stream]
  (let [f' (reify ReduceFunction
             (reduce [_ acc x]
               (f acc x)))]
    (cond
      (instance? KeyedStream stream) (.reduce ^KeyedStream stream f')
      (instance? WindowedStream stream) (.reduce ^WindowedStream stream f'))))

(def flat-map mapcat)

(defn ^KeyedStream group-by [k ^DataStream stream]
  (cond
    (fn? k)
    (let [ks (reify KeySelector
               (getKey [_ x]
                 (k x)))]
      (KeyedStream. stream ^KeySelector ks (u/type-hint Object)))

    (string? k)
    (.keyBy stream ^"[Ljava.lang.String;" (into-array String [k]))

    :else
    (.keyBy stream ^ints (int-array [k]))))

(def key-by group-by)

(defn time-window-assigner [k & args]
  (case k
    :with-gap
    (EventTimeSessionWindows/withGap (coerce-time (first args)))

    :with-dynamic-gap
    (EventTimeSessionWindows/withDynamicGap (first args))))

(defn ^WindowedStream window [^WindowAssigner assigner ^KeyedStream stream]
  (.window stream assigner))

(defn ^WindowedStream time-window
  ([size ^KeyedStream stream]
   (.timeWindow stream (coerce-time size)))
  ([size slide ^KeyedStream stream]
   (.timeWindow stream (coerce-time size) (coerce-time slide))))

(defn ^WindowedStream count-window
  ([size ^KeyedStream stream]
   (.countWindow stream size))
  ([size slide ^KeyedStream stream]
   (.countWindow stream size slide)))

(defn ^SingleOutputStreamOperator sum [k ^DataStream stream]
  (cond
    (instance? KeyedStream stream)
    (cond
      (number? k) (.sum ^KeyedStream stream (int k))
      (string? k) (.sum ^KeyedStream stream ^String k))

    (instance? WindowedStream stream)
    (cond
      (number? k) (.sum ^WindowedStream stream (int k))
      (string? k) (.sum ^WindowedStream stream ^String k))))

(defn ^DataStreamSink print [^DataStream stream]
  (.print stream))

(defn stream [^StreamExecutionEnvironment env spec]
  (let [{:keys [host port del max-retry path charset source source-name type]
         :or   {del "\n" max-retry 0 charset "UTF-8" source-name "Custom source"}} spec]
    (cond
      source
      (if type
        (.addSource env ^SourceFunction source (coerce-type-information type))
        (.addSource env ^SourceFunction source))

      path
      (.readTextFile env path charset)

      :else
      (.socketTextStream env ^String host ^int port ^String del ^long max-retry))))

(defn to-stream
  ([env xs]
   (cond
     (instance? Collection xs)
     (.fromCollection env ^Collection xs)

     (.isArray (.getClass xs))
     (.fromElements env xs)))
  ([env xs type-info]
   (cond
     (instance? Collection xs)
     (.fromCollection env ^Collection xs (coerce-type-information type-info))

     (.isArray (.getClass xs))
     (.fromElements env xs))))

(defn to-file
  ([path ^DataStream stream]
   (.writeAsText stream path))
  ([path mode ^DataStream stream]
   (if-let [mode
            (cond
              (= mode :no-overwrite) FileSystem$WriteMode/NO_OVERWRITE
              (= mode :overwrite) FileSystem$WriteMode/OVERWRITE
              :else nil)]
     (.writeAsText stream path mode)
     (to-file path stream))))