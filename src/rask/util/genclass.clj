; based on original genclass from Clojure

; generic support based on https://github.com/jblomo/clojure/commit/75bf0ce1eb4e33ba26f0a78ca6523fac11ef5e0b
; by Jim Blomo

(ns rask.util.genclass
  (:refer-clojure :exclude [gen-class gen-interface])
  (:import [clojure.lang DynamicClassLoader RT Var IFn ISeq Util]
           [clojure.asm Opcodes ClassWriter Type]
           [java.lang.reflect Modifier]
           [clojure.asm.commons GeneratorAdapter Method]))

;; private functions from clojure.core and genclass
(def into1 #'clojure.core/into1)
(def reduce1 #'clojure.core/reduce1)
(def add-annotations #'clojure.core/add-annotations)
(def non-private-methods #'clojure.core/non-private-methods)
(def protected-final-methods #'clojure.core/protected-final-methods)
(def ctor-sigs #'clojure.core/ctor-sigs)
(def overload-name #'clojure.core/overload-name)
(def find-field #'clojure.core/find-field)
(def prim->class #'clojure.core/prim->class)
(def validate-generate-class-options #'clojure.core/validate-generate-class-options)

(defn- ^Class the-class [x]
  (#'clojure.core/the-class ((ns-map *ns*) x x)))

(defn- iname [^Class c]
  (.getInternalName (Type/getType c)))

(defn- dname [^Class c]
  (.getDescriptor (Type/getType c)))

(defn- signature [^Class c params]
  (if (empty? params)
    (dname c)
    (let [sigs (map #(if (vector? %)
                       (signature (the-class (first %)) (rest %))
                       (signature (the-class %) nil))
                    params)]
      (. (dname c)
         (replace ";" (str \< (apply str sigs) \> \;))))))

(defn- totype [^Class c] (. Type (getType c)))

(defn- to-types
  [cs]
  (if (pos? (count cs))
    (into-array (map totype cs))
    (make-array Type 0)))

(defn generate-class [options-map]
  (validate-generate-class-options options-map)
  (let [default-options {:prefix "-" :load-impl-ns true :impl-ns (ns-name *ns*)}
        {:keys [name extends implements constructors methods main factory state init exposes
                exposes-methods prefix load-impl-ns impl-ns post-init]}
        (merge default-options options-map)
        name-meta (meta name)
        name (str name)
        super (if extends (the-class extends) Object)
        interfaces (map the-class implements)
        supers (cons super interfaces)
        ctor-sig-map (or constructors (zipmap (ctor-sigs super) (ctor-sigs super)))
        cv (new ClassWriter (. ClassWriter COMPUTE_MAXS))
        cname (. name (replace "." "/"))
        impl-pkg-name (str impl-ns)
        impl-cname (.. impl-pkg-name (replace "." "/") (replace \- \_))
        ctype (. Type (getObjectType cname))
        obj-type ^Type (totype Object)
        arg-types (fn [n] (if (pos? n)
                            (into-array (replicate n obj-type))
                            (make-array Type 0)))
        super-type ^Type (totype super)
        class-signature (let [symbols (cons extends implements)]
                          (when (some #(:types (meta %)) symbols)
                            (apply str (map #(signature % (:types (meta %2)))
                                            supers
                                            symbols))))
        init-name (str init)
        post-init-name (str post-init)
        factory-name (str factory)
        state-name (str state)
        main-name "main"
        var-name (fn [s] (Compiler/munge (str s "__var")))
        class-type  (totype Class)
        rt-type  (totype RT)
        var-type ^Type (totype Var)
        ifn-type (totype IFn)
        iseq-type (totype ISeq)
        ex-type  (totype UnsupportedOperationException)
        util-type (totype Util)
        all-sigs (distinct (concat (map #(let[[m p] (key %)] {m [p]}) (mapcat non-private-methods supers))
                                   (map (fn [[m p]] {(str m) [p]}) methods)))
        sigs-by-name (apply merge-with concat {} all-sigs)
        overloads (into1 {} (filter (fn [[m s]] (next s)) sigs-by-name))
        var-fields (concat (when init [init-name])
                           (when post-init [post-init-name])
                           (when main [main-name])
                           ;(when exposes-methods (map str (vals exposes-methods)))
                           (distinct (concat (keys sigs-by-name)
                                             (mapcat (fn [[m s]] (map #(overload-name m (map the-class %)) s)) overloads)
                                             (mapcat (comp (partial map str) vals val) exposes))))
        emit-get-var (fn [^GeneratorAdapter gen v]
                       (let [false-label (. gen newLabel)
                             end-label (. gen newLabel)]
                         (. gen getStatic ctype (var-name v) var-type)
                         (. gen dup)
                         (. gen invokeVirtual var-type (. Method (getMethod "boolean isBound()")))
                         (. gen ifZCmp (. GeneratorAdapter EQ) false-label)
                         (. gen invokeVirtual var-type (. Method (getMethod "Object get()")))
                         (. gen goTo end-label)
                         (. gen mark false-label)
                         (. gen pop)
                         (. gen visitInsn (. Opcodes ACONST_NULL))
                         (. gen mark end-label)))
        emit-unsupported (fn [^GeneratorAdapter gen ^Method m]
                           (. gen (throwException ex-type (str (. m (getName)) " ("
                                                               impl-pkg-name "/" prefix (.getName m)
                                                               " not defined?)"))))
        emit-forwarding-method
        (fn [name pclasses rclass as-static else-gen]
          (let [mname (str name)
                pmetas (map meta pclasses)
                pclasses (map the-class pclasses)
                rclass (the-class rclass)
                ptypes (to-types pclasses)
                rtype ^Type (totype rclass)
                m (new Method mname rtype ptypes)
                is-overload (seq (overloads mname))
                gen (new GeneratorAdapter (+ (. Opcodes ACC_PUBLIC) (if as-static (. Opcodes ACC_STATIC) 0))
                         m nil nil cv)
                found-label (. gen (newLabel))
                else-label (. gen (newLabel))
                end-label (. gen (newLabel))]
            (add-annotations gen (meta name))
            (dotimes [i (count pmetas)]
              (add-annotations gen (nth pmetas i) i))
            (. gen (visitCode))
            (if (> (count pclasses) 18)
              (else-gen gen m)
              (do
                (when is-overload
                  (emit-get-var gen (overload-name mname pclasses))
                  (. gen (dup))
                  (. gen (ifNonNull found-label))
                  (. gen (pop)))
                (emit-get-var gen mname)
                (. gen (dup))
                (. gen (ifNull else-label))
                (when is-overload
                  (. gen (mark found-label)))
                ;if found
                (.checkCast gen ifn-type)
                (when-not as-static
                  (. gen (loadThis)))
                ;box args
                (dotimes [i (count ptypes)]
                  (. gen (loadArg i))
                  (. clojure.lang.Compiler$HostExpr (emitBoxReturn nil gen (nth pclasses i))))
                ;call fn
                (. gen (invokeInterface ifn-type (new Method "invoke" obj-type
                                                      (to-types (replicate (+ (count ptypes)
                                                                              (if as-static 0 1))
                                                                           Object)))))
                ;(into-array (cons obj-type
                ;                 (replicate (count ptypes) obj-type))))))
                ;unbox return
                (. gen (unbox rtype))
                (when (= (. rtype (getSort)) (. Type VOID))
                  (. gen (pop)))
                (. gen (goTo end-label))

                ;else call supplied alternative generator
                (. gen (mark else-label))
                (. gen (pop))

                (else-gen gen m)

                (. gen (mark end-label))))
            (. gen (returnValue))
            (. gen (endMethod))))]

    ;start class definition
    (. cv (visit (. Opcodes V1_5) (+ (. Opcodes ACC_PUBLIC) (. Opcodes ACC_SUPER))
                 cname class-signature (iname super)
                 (when-let [ifc (seq interfaces)]
                   (into-array (map iname ifc)))))

    ; class annotations
    (add-annotations cv name-meta)

    ;static fields for vars
    (doseq [v var-fields]
      (. cv (visitField (+ (. Opcodes ACC_PRIVATE) (. Opcodes ACC_FINAL) (. Opcodes ACC_STATIC))
                        (var-name v)
                        (. var-type getDescriptor)
                        nil nil)))

    ;instance field for state
    (when state
      (. cv (visitField (+ (. Opcodes ACC_PUBLIC) (. Opcodes ACC_FINAL))
                        state-name
                        (. obj-type getDescriptor)
                        nil nil)))

    ;static init to set up var fields and load init
    (let [gen (new GeneratorAdapter (+ (. Opcodes ACC_PUBLIC) (. Opcodes ACC_STATIC))
                   (. Method getMethod "void <clinit> ()")
                   nil nil cv)]
      (. gen (visitCode))
      (doseq [v var-fields]
        (. gen push impl-pkg-name)
        (. gen push (str prefix v))
        (. gen (invokeStatic var-type (. Method (getMethod "clojure.lang.Var internPrivate(String,String)"))))
        (. gen putStatic ctype (var-name v) var-type))

      (when load-impl-ns
        (. gen push (str "/" impl-cname))
        (. gen push ctype)
        (. gen (invokeStatic util-type (. Method (getMethod "Object loadWithClass(String,Class)"))))
        ;        (. gen push (str (.replace impl-pkg-name \- \_) "__init"))
        ;        (. gen (invokeStatic class-type (. Method (getMethod "Class forName(String)"))))
        (. gen pop))

      (. gen (returnValue))
      (. gen (endMethod)))

    ;ctors
    (doseq [[pclasses super-pclasses] ctor-sig-map]
      (let [constructor-annotations (meta pclasses)
            pclasses (map the-class pclasses)
            super-pclasses (map the-class super-pclasses)
            ptypes (to-types pclasses)
            super-ptypes (to-types super-pclasses)
            m (new Method "<init>" (. Type VOID_TYPE) ptypes)
            super-m (new Method "<init>" (. Type VOID_TYPE) super-ptypes)
            gen (new GeneratorAdapter (. Opcodes ACC_PUBLIC) m nil nil cv)
            _ (add-annotations gen constructor-annotations)
            no-init-label (. gen newLabel)
            end-label (. gen newLabel)
            no-post-init-label (. gen newLabel)
            end-post-init-label (. gen newLabel)
            nth-method (. Method (getMethod "Object nth(Object,int)"))
            local (. gen newLocal obj-type)]
        (. gen (visitCode))

        (if init
          (do
            (emit-get-var gen init-name)
            (. gen dup)
            (. gen ifNull no-init-label)
            (.checkCast gen ifn-type)
            ;box init args
            (dotimes [i (count pclasses)]
              (. gen (loadArg i))
              (. clojure.lang.Compiler$HostExpr (emitBoxReturn nil gen (nth pclasses i))))
            ;call init fn
            (. gen (invokeInterface ifn-type (new Method "invoke" obj-type
                                                  (arg-types (count ptypes)))))
            ;expecting [[super-ctor-args] state] returned
            (. gen dup)
            (. gen push (int 0))
            (. gen (invokeStatic rt-type nth-method))
            (. gen storeLocal local)

            (. gen (loadThis))
            (. gen dupX1)
            (dotimes [i (count super-pclasses)]
              (. gen loadLocal local)
              (. gen push (int i))
              (. gen (invokeStatic rt-type nth-method))
              (. clojure.lang.Compiler$HostExpr (emitUnboxArg nil gen (nth super-pclasses i))))
            (. gen (invokeConstructor super-type super-m))

            (if state
              (do
                (. gen push (int 1))
                (. gen (invokeStatic rt-type nth-method))
                (. gen (putField ctype state-name obj-type)))
              (. gen pop))

            (. gen goTo end-label)
            ;no init found
            (. gen mark no-init-label)
            (. gen (throwException ex-type (str impl-pkg-name "/" prefix init-name " not defined")))
            (. gen mark end-label))
          (if (= pclasses super-pclasses)
            (do
              (. gen (loadThis))
              (. gen (loadArgs))
              (. gen (invokeConstructor super-type super-m)))
            (throw (new Exception ":init not specified, but ctor and super ctor args differ"))))

        (when post-init
          (emit-get-var gen post-init-name)
          (. gen dup)
          (. gen ifNull no-post-init-label)
          (.checkCast gen ifn-type)
          (. gen (loadThis))
          ;box init args
          (dotimes [i (count pclasses)]
            (. gen (loadArg i))
            (. clojure.lang.Compiler$HostExpr (emitBoxReturn nil gen (nth pclasses i))))
          ;call init fn
          (. gen (invokeInterface ifn-type (new Method "invoke" obj-type
                                                (arg-types (inc (count ptypes))))))
          (. gen pop)
          (. gen goTo end-post-init-label)
          ;no init found
          (. gen mark no-post-init-label)
          (. gen (throwException ex-type (str impl-pkg-name "/" prefix post-init-name " not defined")))
          (. gen mark end-post-init-label))

        (. gen (returnValue))
        (. gen (endMethod))
        ;factory
        (when factory
          (let [fm (new Method factory-name ctype ptypes)
                gen (new GeneratorAdapter (+ (. Opcodes ACC_PUBLIC) (. Opcodes ACC_STATIC))
                         fm nil nil cv)]
            (. gen (visitCode))
            (. gen newInstance ctype)
            (. gen dup)
            (. gen (loadArgs))
            (. gen (invokeConstructor ctype m))
            (. gen (returnValue))
            (. gen (endMethod))))))

    ;add methods matching supers', if no fn -> call super
    (let [mm (non-private-methods super)]
      (doseq [^java.lang.reflect.Method meth (vals mm)]
        (emit-forwarding-method (.getName meth) (.getParameterTypes meth) (.getReturnType meth) false
                                (fn [^GeneratorAdapter gen ^Method m]
                                  (. gen (loadThis))
                                  ;push args
                                  (. gen (loadArgs))
                                  ;call super
                                  (. gen (visitMethodInsn (. Opcodes INVOKESPECIAL)
                                                          (. super-type (getInternalName))
                                                          (. m (getName))
                                                          (. m (getDescriptor)))))))
      ;add methods matching interfaces', if no fn -> throw
      (reduce1
        (fn [mm ^java.lang.reflect.Method meth]
          (if (contains? mm (method-sig meth))
            mm
            (do
              (emit-forwarding-method (.getName meth) (.getParameterTypes meth) (.getReturnType meth) false
                                      emit-unsupported)
              (assoc mm (method-sig meth) meth))))
        mm (mapcat #(.getMethods ^Class %) interfaces))
      ;extra methods
      (doseq [[mname pclasses rclass :as msig] methods]
        (emit-forwarding-method mname pclasses rclass (:static (meta msig))
                                emit-unsupported))
      ;expose specified overridden superclass methods
      (doseq [[local-mname ^java.lang.reflect.Method m] (reduce1
                                                          (fn [ms [[name _ _] m]]
                                                            (if (contains? exposes-methods (symbol name))
                                                              (conj ms [((symbol name) exposes-methods) m])
                                                              ms))
                                                          []
                                                          (concat (seq mm)
                                                                  (seq (protected-final-methods super))))]
        (let [ptypes (to-types (.getParameterTypes m))
              rtype (totype (.getReturnType m))
              exposer-m (new Method (str local-mname) rtype ptypes)
              target-m (new Method (.getName m) rtype ptypes)
              gen (new GeneratorAdapter (. Opcodes ACC_PUBLIC) exposer-m nil nil cv)]
          (. gen (loadThis))
          (. gen (loadArgs))
          (. gen (visitMethodInsn (. Opcodes INVOKESPECIAL)
                                  (. super-type (getInternalName))
                                  (. target-m (getName))
                                  (. target-m (getDescriptor))))
          (. gen (returnValue))
          (. gen (endMethod)))))
    ;main
    (when main
      (let [m (. Method getMethod "void main (String[])")
            gen (new GeneratorAdapter (+ (. Opcodes ACC_PUBLIC) (. Opcodes ACC_STATIC))
                     m nil nil cv)
            no-main-label (. gen newLabel)
            end-label (. gen newLabel)]
        (. gen (visitCode))

        (emit-get-var gen main-name)
        (. gen dup)
        (. gen ifNull no-main-label)
        (.checkCast gen ifn-type)
        (. gen loadArgs)
        (. gen (invokeStatic rt-type (. Method (getMethod "clojure.lang.ISeq seq(Object)"))))
        (. gen (invokeInterface ifn-type (new Method "applyTo" obj-type
                                              (into-array [iseq-type]))))
        (. gen pop)
        (. gen goTo end-label)
        ;no main found
        (. gen mark no-main-label)
        (. gen (throwException ex-type (str impl-pkg-name "/" prefix main-name " not defined")))
        (. gen mark end-label)
        (. gen (returnValue))
        (. gen (endMethod))))
    ;field exposers
    (doseq [[f {getter :get setter :set}] exposes]
      (let [fld (find-field super (str f))
            ftype (totype (.getType fld))
            static? (Modifier/isStatic (.getModifiers fld))
            acc (+ Opcodes/ACC_PUBLIC (if static? Opcodes/ACC_STATIC 0))]
        (when getter
          (let [m (new Method (str getter) ftype (to-types []))
                gen (new GeneratorAdapter acc m nil nil cv)]
            (. gen (visitCode))
            (if static?
              (. gen getStatic ctype (str f) ftype)
              (do
                (. gen loadThis)
                (. gen getField ctype (str f) ftype)))
            (. gen (returnValue))
            (. gen (endMethod))))
        (when setter
          (let [m (new Method (str setter) Type/VOID_TYPE (into-array [ftype]))
                gen (new GeneratorAdapter acc m nil nil cv)]
            (. gen (visitCode))
            (if static?
              (do
                (. gen loadArgs)
                (. gen putStatic ctype (str f) ftype))
              (do
                (. gen loadThis)
                (. gen loadArgs)
                (. gen putField ctype (str f) ftype)))
            (. gen (returnValue))
            (. gen (endMethod))))))
    ;finish class def
    (. cv (visitEnd))
    [cname (. cv (toByteArray))]))

(defmacro gen-class
  [& options]
  (let [options-map (into1 {} (map vec (partition 2 options)))
        [cname bytecode] (generate-class options-map)]
    (when *compile-files*
      (Compiler/writeClassFile cname bytecode))
    (.defineClass ^DynamicClassLoader (deref Compiler/LOADER)
                  (str (:name options-map)) bytecode options)))

;;;;;;;;;;;;;;;;;;;; gen-interface ;;;;;;;;;;;;;;;;;;;;;;
;; based on original contribution by Chris Houser

(defn- ^Type asm-type
  [c]
  (if (or (instance? Class c) (prim->class c))
    (Type/getType (the-class c))
    (let [strx (str c)]
      (Type/getObjectType
        (.replace (if (some #{\. \[} strx)
                    strx
                    (str "java.lang." strx))
                  "." "/")))))

(defn- generate-interface
  [{:keys [name extends methods]}]
  (when (some #(-> % first clojure.core/name (.contains "-")) methods)
    (throw
      (IllegalArgumentException. "Interface methods must not contain '-'")))
  (let [supers (map the-class extends)
        iname (.replace (str name) "." "/")
        interface-signature (when (some #(get (meta %) :types) extends)
                              (apply str (map #(signature % (get (meta %2) :types))
                                              supers
                                              extends)))
        cv (ClassWriter. ClassWriter/COMPUTE_MAXS)]
    (. cv visit Opcodes/V1_5 (+ Opcodes/ACC_PUBLIC
                                Opcodes/ACC_ABSTRACT
                                Opcodes/ACC_INTERFACE)
       iname interface-signature "java/lang/Object"
       (when (seq extends)
         (into-array (map #(.getInternalName (asm-type %)) extends))))
    (when (not= "NO_SOURCE_FILE" *source-path*) (. cv visitSource *source-path* nil))
    (add-annotations cv (meta name))
    (doseq [[mname pclasses rclass pmetas] methods]
      (let [mv (. cv visitMethod (+ Opcodes/ACC_PUBLIC Opcodes/ACC_ABSTRACT)
                  (str mname)
                  (Type/getMethodDescriptor (asm-type rclass)
                                            (if pclasses
                                              (into-array Type (map asm-type pclasses))
                                              (make-array Type 0)))
                  nil nil)]
        (add-annotations mv (meta mname))
        (dotimes [i (count pmetas)]
          (add-annotations mv (nth pmetas i) i))
        (. mv visitEnd)))
    (. cv visitEnd)
    [iname (. cv toByteArray)]))

(defmacro gen-interface
  [& options]
  (let [options-map (apply hash-map options)
        [cname bytecode] (generate-interface options-map)]
    (when *compile-files*
      (Compiler/writeClassFile cname bytecode))
    (.defineClass ^DynamicClassLoader (deref Compiler/LOADER)
                  (str (:name options-map)) bytecode options)))