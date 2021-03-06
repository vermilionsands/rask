(ns rask.deprecated.type
  (:import [java.util Map List]
           [org.apache.flink.api.common.typeinfo TypeInformation]
           [org.apache.flink.api.java.tuple Tuple]
           [org.apache.flink.api.java.typeutils ListTypeInfo MapTypeInfo TupleTypeInfo]))

(defn- simple-info [c]
  (TypeInformation/of ^Class c))

(defn type-info
  "Creates a TypeInformation instance for a given class and optional generic types.
  If generic types are also generic, enclose them in vector and provide their generic types.

  Samples:
     (type-info List Integer)
     => TypeInformation<List<Integer>>

     (type-info Map String Long)
     => TypeInformation<Map<String, Long>>

     (type-info Map String [List Integer])
     => TypeInformation<Map<String, List<Integer>>"
  ^TypeInformation [c & generics]
  (cond
    (isa? c Tuple)
    (->> generics
         (map #(if (coll? %) (apply type-info %) (type-info %)))
         (into-array TypeInformation)
         (TupleTypeInfo.))

    (and (isa? c Map) (= 2 (count generics)))
    (let [[key-class val-class] generics
          f #(if (coll? %) % [%])]
      (MapTypeInfo.
        ^TypeInformation (apply type-info (f key-class))
        ^TypeInformation (apply type-info (f val-class))))

    (and (isa? c List) (not-empty generics))
    (ListTypeInfo.
      ^TypeInformation (apply type-info generics))

    (empty? generics)
    (simple-info c)

    :else (throw (IllegalArgumentException. (str "Unsupported generic type: " c generics)))))