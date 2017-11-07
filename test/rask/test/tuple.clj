(ns rask.test.tuple
  (:require [clojure.test :refer [deftest is testing]]
            [rask.tuple :as tuple])
  (:import [clojure.lang IPersistentVector]
           [java.util List Map]
           [org.apache.flink.api.java.tuple
            Tuple0 Tuple1 Tuple2 Tuple3 Tuple4 Tuple5 Tuple6 Tuple7 Tuple8 Tuple9 Tuple10 Tuple11
            Tuple12 Tuple13 Tuple14 Tuple15 Tuple16 Tuple17 Tuple18 Tuple19 Tuple20 Tuple21 Tuple22
            Tuple23 Tuple24 Tuple25 Tuple]
           [org.apache.flink.api.java.typeutils TupleTypeInfo]))

(defn type-info->class [^TupleTypeInfo hint index]
  (.getTypeClass (.getTypeAt hint ^int index)))

(defmacro tuple-test [c arity]
  `(let [xs# (vec (range ~arity))
         t# (apply tuple/tuple xs#)
         ys# (tuple/as-vector t#)]
     (is (instance? ~c t#))
     (is (= xs# ys#))))

(deftest tuple-creation-test
  (testing "Creating different arities"
    (is (instance? Tuple0 (tuple/tuple)))
    (tuple-test Tuple1 1)
    (tuple-test Tuple2 2)
    (tuple-test Tuple3 3)
    (tuple-test Tuple4 4)
    (tuple-test Tuple5 5)
    (tuple-test Tuple6 6)
    (tuple-test Tuple7 7)
    (tuple-test Tuple8 8)
    (tuple-test Tuple9 9)
    (tuple-test Tuple10 10)
    (tuple-test Tuple11 11)
    (tuple-test Tuple12 12)
    (tuple-test Tuple13 13)
    (tuple-test Tuple14 14)
    (tuple-test Tuple15 15)
    (tuple-test Tuple16 16)
    (tuple-test Tuple17 17)
    (tuple-test Tuple18 18)
    (tuple-test Tuple19 19)
    (tuple-test Tuple20 20)
    (tuple-test Tuple21 21)
    (tuple-test Tuple22 22)
    (tuple-test Tuple23 23)
    (tuple-test Tuple24 24)
    (tuple-test Tuple25 25)))

(deftest into-tuple-test
  (testing "Creating empty tuple"
    (let [t (tuple/into-tuple [])]
      (is (instance? Tuple0 t))))
  (testing "Creating tuple with some content"
    (let [t (tuple/into-tuple [1 2 3])]
      (is (instance? Tuple3 t))
      (is (= [1 2 3] (tuple/as-vector t)))))
  (testing "Creating tuple with more than 25 elements should fail"
    (is (thrown? IllegalArgumentException (tuple/into-tuple (range 50))))))

(deftest as-vector-test
  (is (= [] (tuple/as-vector (tuple/tuple))))
  (is (= [1 2 3] (tuple/as-vector (tuple/tuple 1 2 3)))))

(deftest nth-test
  (is (= 1 (tuple/nth (tuple/tuple 1 2) 0)))
  (is (= 100 (tuple/nth (tuple/tuple) 0 100))))

(deftest fields-test
  (is (= [] (tuple/fields (tuple/tuple 1 2 3) [])))
  (is (= [1 2] (tuple/fields (tuple/tuple 1 2 3) [0 1])))
  (is (= [1 1 2 2] (tuple/fields (tuple/tuple 1 2 3) [0 0 1 1]))))

(deftest tuple-type-info-test
  (testing "Hint for basic types"
    (let [type-info (tuple/tuple-info Integer String)]
      (is (= Integer (type-info->class type-info 0)))
      (is (= String (type-info->class type-info 1)))))
  (testing "Hint for Clojure types"
    (let [type-info (tuple/tuple-info IPersistentVector)]
      (is (= IPersistentVector (type-info->class type-info 0)))))
  (testing "Hint for generic types"
    (let [type-info (tuple/tuple-info List)]
      (is (= List (type-info->class type-info 0)))))
  (testing "Hint for nested tuple"
    (let [type-info (tuple/tuple-info [Tuple Integer Map] [Tuple Long Long Long])]
      (is (= Tuple2 (type-info->class type-info 0)))
      (is (= Tuple3 (type-info->class type-info 1))))))