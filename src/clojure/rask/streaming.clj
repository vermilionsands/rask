(ns rask.streaming
  (:refer-clojure :exclude [group-by map mapcat print reduce])
  (:require [rask.util :as u])
  (:import [org.apache.flink.api.common.functions FlatMapFunction MapFunction ReduceFunction]
           [org.apache.flink.api.common.typeinfo TypeHint TypeInformation]
           [org.apache.flink.api.java.functions KeySelector]
           [org.apache.flink.streaming.api.datastream DataStream DataStreamSink KeyedStream SingleOutputStreamOperator WindowedStream]
           [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.streaming.api.windowing.time Time]
           [org.apache.flink.util Collector]))

;; todo add remote and configuration options
(defn env
  ([]
   (env nil))
  ([options]
   (cond
     (:local? options)
     (StreamExecutionEnvironment/createLocalEnvironment)

     :else
     (StreamExecutionEnvironment/getExecutionEnvironment))))

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

(defn ^WindowedStream time-window
  ([size ^KeyedStream stream]
   (let [size' (if (instance? Time size) size (Time/milliseconds size))]
     (.timeWindow stream size')))
  ([size slide ^KeyedStream stream]
   (let [size'  (if (instance? Time size) size (Time/milliseconds size))
         slide' (if (instance? Time slide) size (Time/milliseconds slide))]
     (.timeWindow stream size' slide'))))

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

;; todo
;; string from socket
;; string from file
;; collection
;; iterator
;; object stream from collection
;; source - kafka? fn?
(defn stream [^StreamExecutionEnvironment env spec]
  (let [{:keys [host port del max-retry]
         :or   {del "\n" max-retry 0}} spec]
    (.socketTextStream env ^String host ^int port ^String del ^long max-retry)))