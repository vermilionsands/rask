(ns rask.data
  (:require [rask.util.genclass :as genclass])
  (:import [org.apache.flink.api.java.tuple Tuple2 Tuple3 Tuple4 Tuple5 Tuple6 Tuple7 Tuple8 Tuple9 Tuple10 Tuple0
                                            Tuple1 Tuple11 Tuple12 Tuple13 Tuple14 Tuple15 Tuple16]
           [clojure.lang DynamicClassLoader]))

(defn tuple
  "Creates a Flink tuple of type based on number number of arguments.

  Supports creating tuples from Tuple0 to Tuple16."
  ([]
   (Tuple0.))
  ([x]
   (Tuple1. x))
  ([x1 x2]
   (Tuple2. x1 x2))
  ([x1 x2 x3 & xs]
   (let [[x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16] xs]
     (condp = (count xs)
       0  (Tuple3.  x1 x2 x3)
       1  (Tuple4.  x1 x2 x3 x4)
       2  (Tuple5.  x1 x2 x3 x4 x5)
       3  (Tuple6.  x1 x2 x3 x4 x5 x6)
       4  (Tuple7.  x1 x2 x3 x4 x5 x6 x7)
       5  (Tuple8.  x1 x2 x3 x4 x5 x6 x7 x8)
       6  (Tuple9.  x1 x2 x3 x4 x5 x6 x7 x8 x9)
       7  (Tuple10. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10)
       8  (Tuple11. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11)
       9  (Tuple12. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12)
       10 (Tuple13. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13)
       11 (Tuple14. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14)
       12 (Tuple15. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15)
       13 (Tuple16. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16)))))

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
    (when true
      (Compiler/writeClassFile cname bytecode))
    (.defineClass ^DynamicClassLoader @Compiler/LOADER (str (:name options-map)) bytecode nil)
    `(new ~classname)))