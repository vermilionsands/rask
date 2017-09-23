(ns rask.api)

(defmacro defraskfn
  "Like defn, but stores its namespace symbol in metadata under :rask.api/namespace."
  [name & body]
  `(def ~name
     (with-meta
       (fn ~@body)
       {::namespace (ns-name *ns*)})))