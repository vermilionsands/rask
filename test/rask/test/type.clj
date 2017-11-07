(ns rask.test.type
  (:require [clojure.test :refer [deftest is testing]]
            [rask.type :as type])
  (:import [clojure.lang PersistentList]
           [java.util List Map]
           [org.apache.flink.api.java.tuple Tuple2]
           [org.apache.flink.api.java.typeutils ListTypeInfo MapTypeInfo TupleTypeInfo]))

(deftest type-info-test
  (testing "Hint for basic type"
    (is (= Integer (.getTypeClass (type/type-info Integer)))))
  (testing "Hint for Clojure type"
    (is (= PersistentList (.getTypeClass (type/type-info PersistentList)))))
  (testing "Hint for generic type without type"
    (is (= List (.getTypeClass (type/type-info List)))))
  (testing "Hint for list with generic type"
    (let [type-info (type/type-info List String)]
      (is (instance? ListTypeInfo type-info))
      (is (= String (.getTypeClass (.getElementTypeInfo ^ListTypeInfo type-info))))))
  (testing "Hint for map"
    (let [type-info (type/type-info Map String [List Integer])]
      (is (instance? MapTypeInfo type-info))
      (is (= String (.getTypeClass (.getKeyTypeInfo ^MapTypeInfo type-info))))
      (is (= Integer (.getTypeClass
                       (.getElementTypeInfo
                         ^ListTypeInfo (.getValueTypeInfo ^MapTypeInfo type-info)))))))
  (testing "Hint for tuple"
    (let [type-info (type/type-info Tuple2 String Integer)]
      (is (instance? TupleTypeInfo type-info))
      (is (= String (.getTypeClass (.getTypeAt ^TupleTypeInfo type-info 0))))
      (is (= Integer (.getTypeClass (.getTypeAt ^TupleTypeInfo type-info 1)))))))