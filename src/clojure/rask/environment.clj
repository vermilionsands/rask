(ns rask.environment
  (:require [clojure.walk :as walk])
  (:import [java.util Collection]
           [org.apache.flink.streaming.api.environment StreamExecutionEnvironment RemoteStreamEnvironment]
           [org.apache.flink.streaming.api CheckpointingMode]
           [org.apache.flink.streaming.api.datastream DataStreamSource]
           [org.apache.flink.api.common.typeinfo TypeInformation]
           [org.apache.flink.api.java.utils ParameterTool]))

;; --------------------------------------------------------------------------------------------------------
;; environment
;; --------------------------------------------------------------------------------------------------------

(defn ^StreamExecutionEnvironment env
  "Creates an execution environment that represents the context in which the program is currently executed."
  []
  (StreamExecutionEnvironment/getExecutionEnvironment))

(defn local-env
  "Creates a LocalStreamEnvironment."
  ([]
   (StreamExecutionEnvironment/createLocalEnvironment))
  ([conf]
   (StreamExecutionEnvironment/createLocalEnvironment
     (StreamExecutionEnvironment/getDefaultLocalParallelism) conf)))

(defn remote-env
  "Creates a RemoteStreamEnvironment."
  ([host port jars]
   (StreamExecutionEnvironment/createRemoteEnvironment host port (into-array String jars)))
  ([host port conf jars]
   (RemoteStreamEnvironment. host port conf (into-array String jars))))

(defn local-env-with-ui
  "Creates a LocalStreamEnvironment for local program execution that also starts the web monitoring UI."
  [conf]
  (StreamExecutionEnvironment/createLocalEnvironmentWithWebUI conf))

(defn execute
  "Triggers the program execution. The environment will execute all parts of the program that have resulted
  in a \"sink\" operation. Sink operations are for example printing results or forwarding them to a message queue.

  The program execution will be logged and displayed with a generated default name or passed job-name."
  ([env]
   (.execute env))
  ([env job-name]
   (.execute env job-name)))

;; --------------------------------------------------------------------------------------------------------
;; configuration
;; --------------------------------------------------------------------------------------------------------

(defn global-job-params
  "Gets/sets global job parameters.
  Accepts a map of keyword -> string"
  ([env]
   (.getGlobalJobParameters
     (.getConfig env)))
  ([env args]
   (.setGlobalJobParameters
     (.getConfig env)
     (ParameterTool/fromMap (walk/stringify-keys args)))
   env))

(defn parallelism
  "Gets/sets the parallelism for operations executed through this environment."
  ([env]
   (.getParallelism env))
  ([env n]
   (.setParallelism env n)))

(defn max-parallelism
  "Gets/sets the maximum degree of parallelism defined for the program."
  ([env]
   (.getMaxParallelism env))
  ([env n]
   (.setMaxParallelism env n)))

(defn buffer-timeout
  "Gets/sets the maximum time frequency (milliseconds) for the flushing of the output buffers."
  ([env]
   (.getBufferTimeout env))
  ([env n]
   (.setBufferTimeout env n)))

(defn chaining? [env]
  (.isChainingEnabled env))

(defn disable-chaining [env]
  (.disableOperatorChaining env))

(defn checkpointing-mode
  [mode]
  (if (keyword? mode)
    (mode {:exactly-once  CheckpointingMode/EXACTLY_ONCE
           :at-least-once CheckpointingMode/AT_LEAST_ONCE})
    mode))

(defn checkpointing
  ([env]
   (.getCheckpointConfig env))
  ([env interval]
   (.enableCheckpointing env interval))
  ([env interval mode]
   (.enableCheckpointing env interval (checkpointing-mode mode))))

;; datastreams

(defn string-stream-from-socket
  ([env host port]
   (.socketTextStream env host port))
  ([env host port del]
   (.socketTextStream env host port del))
  ([env host port del max-retry]
   (.socketTextStream env host port del max-retry)))

(defn string-stream-from-file
  ([env path]
   (.readTextFile env path))
  ([env path charset]
   (.readTextFile env path charset)))

(defn ^DataStreamSource stream-from-collection
  ([env xs]
   (.fromCollection env ^Collection xs))
  ([env xs type-info]
   (let [type-info (if (instance? TypeInformation type-info)
                     type-info
                     (TypeInformation/of ^Class type-info))]
     (cond
       (instance? Collection xs)
       (.fromCollection env ^Collection xs ^TypeInformation type-info)

       (.isArray (.getClass xs))
       (.fromElements env xs)))))

(defn object-stream-from-collection
  [env xs]
  (.fromCollection env xs (TypeInformation/of ^Class Object)))