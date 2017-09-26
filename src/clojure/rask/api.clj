(ns rask.api)

(defmacro raskfn
  "Like fn, but stores it's namespace symbol in metadata under :rask.api/namespace."
  [& body]
  (let [namespace (ns-name *ns*)]
    `(with-meta
       (fn ~@body)
       {::namespace '~namespace})))

(defmacro defraskfn
  "Like defn, but stores it's namespace symbol in metadata under :rask.api/namespace."
  [name & body]
  `(def ~name
     (raskfn ~@body)))
