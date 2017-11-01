(ns rask.api
  (:import [clojure.lang Compiler$LocalBinding]
           [rask.util SerializableFn]
           [java.util.concurrent.atomic AtomicReference]))

;; there should be a simpler way to do this...

(defn make-serializable
  ([f]
   (SerializableFn. f))
  ([fn-name form]
   (SerializableFn. fn-name form))
  ([f fn-name form]
   (SerializableFn. f fn-name form)))

(defn- generate-name [namespace fn-name line column]
  (let [ns-string (name namespace)]
    (if fn-name
      (symbol ns-string (name fn-name))
      (let [ns-name (last (clojure.string/split ns-string #"\."))
            fn-name (str \_ ns-name \- line \- column)]
        (symbol ns-string fn-name)))))

(defn used-symbols [form]
  (->> form
    (tree-seq coll? seq)
    (filter symbol?)
    (set)))

(defn- bindings [local-bindings used-symbols-set]
  (->>
    (filter
      (fn [^Compiler$LocalBinding b]
        (used-symbols-set (.sym b)))
      local-bindings)
    (mapcat
      (fn [^Compiler$LocalBinding b]
        [(list symbol (name (.sym b)))
         (.sym b)]))
    vec))

;;todo add bindings without let
(defmacro raskfn
  "Like defn, but tries to store it's form with local bindings in meta."
  [& body]
  (let [namespace (ns-name *ns*)
        form &form
        {:keys [line column]} (meta &form)
        fn-name (-> body first meta ::name)
        fn-name (generate-name namespace fn-name line column)
        local-bindings (bindings (vals &env) (used-symbols (rest body)))]
    `(make-serializable
       (fn ~@body)
       '~fn-name
       (list 'let ~(vec local-bindings) '~form))))

(defmacro defraskfn
  "Like defn, but tries to store it's form with local bindings in meta."
  [name & body]
  (let [[x & xs] body
        x (with-meta x {:rask/name name})]
    `(def ~name
       (raskfn ~x ~@xs))))

;;todo remove locking
(defn iterator
  [state next & [has-next]]
  (let [state (AtomicReference. state)]
    (reify
      java.io.Serializable
      java.util.Iterator
      (hasNext [_]
        (locking state
          ((or has-next (constantly true)) (.get state))))
      (next [_]
        (locking state
          (let [[x & state'] (next (.get state))]
            (.set state state')
            x))))))