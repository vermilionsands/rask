(ns rask.test.api
  (:require [clojure.test :refer [deftest is testing]]
            [rask.api]))

(deftest sfn-is-serializable-test)
(deftest sfn-is-a-function-test)
(deftest serialization-test)
(deftest deserialization-test)
(deftest namespace-init-test)

;;named
;;anonymous
;;local binding
;;reify etc w binding
;;namespace init