(ns rask.deprecated.util
  (:refer-clojure :exclude [time])
  (:require [clojure.walk :as walk])
  (:import [java.util.concurrent TimeUnit]
           [org.apache.flink.streaming.api.windowing.time Time]
           [org.apache.flink.api.java.utils ParameterTool]
           [rask.deprecated.timestamps AscendingTimestampFn BoundedOutOfOrdnessFn]))

(defn parse-args
  "Parse an array of strings into a map of keyword -> string"
  [args]
  (let [params
        (if args
          (into {} (.toMap (ParameterTool/fromArgs (into-array String args))))
          {})]
    (walk/keywordize-keys params)))

(defn time
  "Accepts either an instance of org.apache.flink.streaming.api.windowing.time.Time which would be returned or
  a number and time unit, from which a new instance of Time would be created.

  When no time-unit is passed defaults to milliseconds."
  ([ms-or-instance]
   (if (instance? Time ms-or-instance)
     ms-or-instance
     (Time/of ms-or-instance TimeUnit/MILLISECONDS)))
  ([size ^TimeUnit time-unit]
   (Time/of size time-unit)))

(defn time-unit
  "Returns a java.util.concurrent TimeUnit based on keyword

  Accepts :nanoseconds, :microseconds, :milliseconds, :seconds, :minutes, :hours, :days."
  [x]
  (cond = x
    :nanoseconds TimeUnit/NANOSECONDS
    :microseconds TimeUnit/MICROSECONDS
    :milliseconds TimeUnit/MILLISECONDS
    :seconds TimeUnit/SECONDS
    :minutes TimeUnit/MINUTES
    :hours TimeUnit/HOURS
    :days TimeUnit/DAYS))

(defn ascending-assigner
  "Creates an instance of AscendingTimestampExtractor that will use function f to extact timestamps.

  f should take one argument and return long."
  [f]
  (AscendingTimestampFn. f))

(defn out-of-ordness-assigner
  "Creates an instance of BoundedOutOfOrdernessTimestampExtractor that will use function f to extact timestamps.

  f should take one argument and return long.
  max-time should be an instance of org.apache.flink.streaming.api.windowing.time.Time or long (for milliseconds)."
  [f max-time]
  (BoundedOutOfOrdnessFn. f (time max-time)))