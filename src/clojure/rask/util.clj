(ns rask.util
  (:refer-clojure :exclude [fn nth])
  (:import [clojure.lang IFn Fn IObj]
           [org.apache.flink.api.java.tuple Tuple0 Tuple1 Tuple2 Tuple3 Tuple4 Tuple5 Tuple6 Tuple7 Tuple8 Tuple9
                                            Tuple10 Tuple11 Tuple12 Tuple13 Tuple14 Tuple15 Tuple16 Tuple17 Tuple18
                                            Tuple19 Tuple20 Tuple21 Tuple22 Tuple23 Tuple24 Tuple25 Tuple]
           [org.apache.flink.api.java.typeutils TupleTypeInfo MapTypeInfo ListTypeInfo]
           [org.apache.flink.api.common.typeinfo TypeInformation]
           [java.io Serializable]
           [java.util List Map]))

(defn tuple
  ([]
   (Tuple0.))
  ([x]
   (Tuple1. x))
  ([x1 x2]
   (Tuple2. x1 x2))
  ([x1 x2 x3]
   (Tuple3. x1 x2 x3))
  ([x1 x2 x3 x4]
   (Tuple4. x1 x2 x3 x4))
  ([x1 x2 x3 x4 x5]
   (Tuple5. x1 x2 x3 x4 x5))
  ([x1 x2 x3 x4 x5 x6 & xs]
   (let [[x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25] xs]
     (condp = (+ (count xs) 6)
       6  (Tuple6. x1 x2 x3 x4 x5 x6)
       7  (Tuple7. x1 x2 x3 x4 x5 x6 x7)
       8  (Tuple8. x1 x2 x3 x4 x5 x6 x7 x8)
       9  (Tuple9. x1 x2 x3 x4 x5 x6 x7 x8 x9)
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
           (format "Can't create Tuple with more than 25 arguments. Too many arguments: %s."
                   (+ (count xs) 6))))))))

(defn nth
  ([^Tuple tuple index]
   (nth tuple index nil))
  ([^Tuple tuple index not-found]
   (or
     (try
       (.getField tuple index)
       (catch IndexOutOfBoundsException _ nil))
     not-found)))

(defn type-hint ^TypeInformation [c & generics]
  (cond
    (isa? c Tuple)
    (->> generics
         (map #(if (coll? %) (apply type-hint %) (type-hint %)))
         (into-array TypeInformation)
         (TupleTypeInfo.))

    (and (isa? c Map) (= 2 (count generics)))
    (let [[key-class val-class] generics
          f #(if (coll? %) % [%])]
      (MapTypeInfo.
        ^TypeInformation (apply type-hint (f key-class))
        ^TypeInformation (apply type-hint (f val-class))))

    (and (isa? c List) (not-empty generics))
    (ListTypeInfo.
      ^TypeInformation (apply type-hint generics))

    (empty? generics)
    (TypeInformation/of ^Class c)

    :else (throw (IllegalArgumentException. (str "Unsupported generic type: " c generics)))))

(defn tuple-hint ^TypeInformation [& classes]
  (apply type-hint Tuple classes))

(definterface ReadResolve
  (readResolve []))

(deftype SerializableFn [^IFn f source-ns]
  Serializable
  Fn

  IFn
  (invoke [_] (.invoke f))
  (invoke [_ x] (.invoke f x))
  (invoke [_ x0 x1] (.invoke f x0 x1))
  (invoke [_ x0 x1 x2] (.invoke f x0 x1 x2))
  (invoke [_ x0 x1 x2 x3] (.invoke f x0 x1 x2 x3))
  (invoke [_ x0 x1 x2 x3 x4] (.invoke f x0 x1 x2 x3 x4))
  (invoke [_ x0 x1 x2 x3 x4 x5] (.invoke f x0 x1 x2 x3 x4 x5))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6] (.invoke f x0 x1 x2 x3 x4 x5 x6))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x12 x13))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x12 x13 x14))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x12 x13 x14 x15))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x12 x13 x14 x15 x16))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19))
  (invoke [_ x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 more] (.invoke f x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 more))

  (applyTo [_ args] (.applyTo f args))

  IObj
  (withMeta [_ m] (.withMeta ^IObj f m))

  (meta [_] (.meta ^IObj f))

  Runnable
  (run [_] (.run f))

  Callable
  (call [_] (.call f))

  ReadResolve
  (readResolve [this]
    (require source-ns)
    this))

(defmacro fn [& body]
  (let [namespace (ns-name *ns*)]
    `(->SerializableFn (clojure.core/fn ~@body) '~namespace)))