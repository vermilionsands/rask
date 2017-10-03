(ns rask.api
  (:import [clojure.lang Compiler$LocalBinding]))

;; there should be a simpler way to do this...

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

(defn bindings [local-bindings used-symbols-set]
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

(defmacro raskfn
  "Like defn, but tries to store it's form with local bindings in meta."
  [& body]
  (let [namespace (ns-name *ns*)
        form &form
        {:keys [line column]} (meta &form)
        fn-name (-> body first meta ::name)
        fn-name (generate-name namespace fn-name line column)
        local-bindings (bindings (vals &env) (used-symbols (rest body)))]
    `(do
       (with-meta
         (fn ~@body)
         {::name '~fn-name
          ::form (list 'let ~(vec local-bindings) '~form)}))))

(defmacro defraskfn
  "Like defn, but tries to store it's form with local bindings in meta."
  [name & body]
  (let [[x & xs] body
        x (with-meta x {::name name})]
    `(def ~name
       (raskfn ~x ~@xs))))
