(ns rask.api
  (:import [clojure.lang Compiler$LocalBinding]
           [rask.util SerializableFn]
           [java.util.concurrent.atomic AtomicReference]))

(defn- serializable-fn
  [f]
  (SerializableFn. f))

(defn- generate-name [namespace line column fn-name]
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

(defmacro sfn
  "Serializable function.

  Like fn, but stores it's form with local bindings in metadata, for further use in
  serialization and deserialization."
  [& body]
  (let [namespace (ns-name *ns*)
        form &form
        {:keys [line column]} (meta &form)
        fn-name (->> (-> body first meta :rask.api/name)
                     (generate-name namespace line column))
        local-bindings (bindings (vals &env) (used-symbols (rest body)))]
    `(serializable-fn
       (with-meta
         (fn ~@body)
         {:rask.api/name     '~fn-name
          :rask.api/form     '~form
          :rask.api/bindings ~local-bindings}))))

(defmacro defsfn
  "Like defn, but serializable. See sfn."
  [name & body]
  (let [[x & xs] body
        x (with-meta x {:rask.api/name name})]
    `(def ~name
       (sfn ~x ~@xs))))

(defn iterator
  [state next & [has-next]]
  (let [state (AtomicReference. state)]
    (reify
      java.io.Serializable
      java.util.Iterator
      (hasNext [_]
        ((or has-next (constantly true)) (.get state)))
      (next [_]
        (let [[x & state'] (next (.get state))]
          (.set state state')
          x)))))