(ns rask.tuple
  (:require [rask.util :as util])
  (:import [org.apache.flink.api.java.tuple Tuple0 Tuple1 Tuple2 Tuple3 Tuple4 Tuple5 Tuple6 Tuple7 Tuple8 Tuple9
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
  ([x1 x2 x3 & xs]
   (let [[x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25] xs]
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
       13 (Tuple16. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16)
       14 (Tuple17. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17)
       15 (Tuple18. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18)
       16 (Tuple19. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19)
       17 (Tuple20. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20)
       18 (Tuple21. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21)
       19 (Tuple22. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22)
       20 (Tuple23. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23)
       21 (Tuple24. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24)
       22 (Tuple25. x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25)
       (throw
         (IllegalArgumentException.
           (format "Cannot create Tuple with more than 25 arguments. Too many arguments: %s."
                   (+ (count xs) 3))))))))

(defmacro tuple-hint
  "TypeHint for flink Tuple. Tuple class would be determined based on count of generic types."
  [& generic-types]
  (let [c (Tuple/getTupleClass (count generic-types))]
    `(util/type-hint ~c ~@generic-types)))