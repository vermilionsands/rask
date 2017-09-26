(ns rask.api)

(defmacro raskfn
  "Like fn, but stores it's namespace symbol in metadata under :rask.api/namespace."
  [& body]
  `(with-meta
     (fn ~@body)
     {::namespace (ns-name *ns*)}))

(defmacro defraskfn
  "Like defn, but stores it's namespace symbol in metadata under :rask.api/namespace."
  [name & body]
  `(def ~name
     (raskfn ~@body)))
