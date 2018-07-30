(ns rask.deprecated.tuple
  (:refer-clojure :exclude [nth])
  (:require [rask.deprecated.type :as types])
  (:import [org.apache.flink.api.common.typeinfo TypeInformation]
           [org.apache.flink.api.java.tuple
            Tuple0 Tuple1 Tuple2 Tuple3 Tuple4 Tuple5 Tuple6 Tuple7 Tuple8 Tuple9
            Tuple10 Tuple11 Tuple12 Tuple13 Tuple14 Tuple15 Tuple16 Tuple17 Tuple18
            Tuple19 Tuple20 Tuple21 Tuple22 Tuple23 Tuple24 Tuple25 Tuple]))

(defn tuple
  "Creates a Flink tuple of type based on number number of arguments.

  Supports creating tuples from Tuple0 to Tuple25."
  ([]
   (Tuple0.))
  ([x]
   (Tuple1. x))
  ([x1 x2]
   (Tuple2. x1 x2))
  ([x1 x2 x3]
   (Tuple3.  x1 x2 x3))
  ([x1 x2 x3 x4]
   (Tuple4.  x1 x2 x3 x4))
  ([x1 x2 x3 x4 x5]
   (Tuple5.  x1 x2 x3 x4 x5))
  ([x1 x2 x3 x4 x5 x6 & xs]
   (let [[x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25] xs]
     (condp = (+ (count xs) 6)
       6  (Tuple6.  x1 x2 x3 x4 x5 x6)
       7  (Tuple7.  x1 x2 x3 x4 x5 x6 x7)
       8  (Tuple8.  x1 x2 x3 x4 x5 x6 x7 x8)
       9  (Tuple9.  x1 x2 x3 x4 x5 x6 x7 x8 x9)
       10 (Tuple10. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10)
       11 (Tuple11. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11)
       12 (Tuple12. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12)
       13 (Tuple13. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13)
       14 (Tuple14. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14)
       15 (Tuple15. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15)
       16 (Tuple16. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16)
       17 (Tuple17. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17)
       18 (Tuple18. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18)
       19 (Tuple19. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19)
       20 (Tuple20. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20)
       21 (Tuple21. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21)
       22 (Tuple22. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22)
       23 (Tuple23. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23)
       24 (Tuple24. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24)
       25 (Tuple25. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25)
       (throw
         (IllegalArgumentException.
           (format "Cannot create Tuple with more than 25 arguments. Too many arguments: %s."
                   (+ (count xs) 6))))))))

(defn into-tuple
  "Creates a tuple containing the contents of coll."
  [coll]
  (apply tuple coll))

(defn as-vector
  "Returns tuple fields as vector."
  [^Tuple tuple]
  (mapv #(.getField tuple %) (range (.getArity tuple))))

(defn nth
  "Gets the field at the specified position.

  If field is nil and not-found was specified would return not-found."
  ([^Tuple tuple index]
   (nth tuple index nil))
  ([^Tuple tuple index not-found]
   (or
     (try
       (.getField tuple index)
       (catch IndexOutOfBoundsException _ nil))
     not-found)))

(defn fields
  "Returns a vector containing fields specified by the indexes from the tuple.
  Same indexes can appear multiple times.

     (fields (tuple 1 2) [0 0 1 1]) => [1 1 2 2]"
  [^Tuple tuple indexes]
  (mapv #(.getField tuple %) indexes))

(defn tuple-info
  "Creates a TypeInformation instance for Tuple of given classes."
  ^TypeInformation
  [& classes]
  (apply types/type-info Tuple classes))