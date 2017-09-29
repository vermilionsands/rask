(ns rask.util
  (:refer-clojure :exclude [time])
  (:require [clojure.walk :as walk]
            [rask.util.genclass :as genclass])
  (:import [clojure.lang DynamicClassLoader]
           [java.util.concurrent TimeUnit]
           [org.apache.flink.streaming.api.windowing.time Time]
           [org.apache.flink.api.java.utils ParameterTool]))

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

(defmacro type-hint
  "Expands to code that defines a class implementing org.apache.flink.api.common.typeinfo.TypeHint<T>
  and returns an instance of this class

  c - class T
  generic-types - additional classes that would be applied to type as generic types.
                  If additional classes are also generic, enclose them in vector.

  For example:
  (type-hint java.util.List String)
  -> TypeHint<java.util.List<String>>
  (type-hint org.apache.flink.api.java.tuple.Tuple2 String Long)
  -> Tuple2<String, Long>
  (type-hint java.util.HashMap Integer [java.util.List String])
  -> TypeHint<java.util.HashMap<Integer,java.util.List<String>>"
  [c & generic-types]
  (let [all-types (if (seq generic-types)
                    [(vec (concat [c] generic-types))]
                    [c])
        ns-part (namespace-munge *ns*)
        classname (symbol (str ns-part "." (gensym "TypeHint")))
        extends
        (with-meta
          'org.apache.flink.api.common.typeinfo.TypeHint
          {:types all-types})
        options-map {:name classname :extends extends}
        [cname bytecode] (genclass/generate-class options-map)]
    (when *compile-files*
      (Compiler/writeClassFile cname bytecode))
    (.defineClass ^DynamicClassLoader @Compiler/LOADER (str (:name options-map)) bytecode nil)
    `(new ~classname)))