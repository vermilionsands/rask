package rask.deprecated.util;

import clojure.lang.*;
import clojure.lang.Compiler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class SerializableFn implements IFn, Fn, IObj, Serializable {

  private static final String CORE_NS = "clojure.core";
  private static final String RASK_NS = "rask.api";
  private static final Var REQUIRE = RT.var(CORE_NS, "require");
  private static final Var FIND_NS = RT.var(CORE_NS, "find-ns");
  private static final Keyword FN_NAME_KEY = Keyword.intern(RASK_NS, "name");
  private static final Keyword FORM_KEY = Keyword.intern(RASK_NS, "form");

  private final Symbol sym;
  private final PersistentList form;
  private transient IFn impl;

  private static synchronized void optionalRequire(Symbol ns) {
    if (FIND_NS.invoke(ns) == null) {
      REQUIRE.invoke(ns);
    }
  }

  private static IFn initFn(Symbol fn, PersistentList form) {
    IFn fnImpl;
    Symbol ns = Symbol.intern(fn.getNamespace());
    optionalRequire(ns);
    Var x = RT.var(fn.getNamespace(), fn.getName());
    if (x.isBound()) {
      fnImpl = x;
    } else {
      Namespace nsObject = (Namespace)FIND_NS.invoke(ns);
      Var.pushThreadBindings(
          PersistentHashMap.create(
              Var.intern(Symbol.intern(CORE_NS), Symbol.intern("*ns*")).setDynamic(),
              nsObject));
      fnImpl = (IFn) Compiler.eval(form);
      Var.popThreadBindings();
    }
    return fnImpl;
  }

  private SerializableFn(IFn fn, Symbol sym, PersistentList form) {
    this.sym = sym;
    this.form = form;
    this.impl = fn;
  }

  public SerializableFn(IFn fn) {
    this(fn, (Symbol) RT.get(RT.meta(fn), FN_NAME_KEY), (PersistentList) RT.get(RT.meta(fn), FORM_KEY));
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.impl = initFn(sym, form);
  }

  @Override
  public Object invoke() {
    return impl.invoke();
  }

  @Override
  public Object invoke(Object o) {
    return impl.invoke(o);
  }

  @Override
  public Object invoke(Object o, Object o1) {
    return impl.invoke(o, o1);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2) {
    return impl.invoke(o, o1, o2);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3) {
    return impl.invoke(o, o1, o2, o3);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4) {
    return impl.invoke(o, o1, o2, o3, o4);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5) {
    return impl.invoke(o, o1, o2, o3, o4, o5);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12, Object o13) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12, Object o13, Object o14) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13, o14);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12, Object o13, Object o14, Object o15) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13, o14, o15);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12, Object o13, Object o14, Object o15, Object o16) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13, o14, o15, o16);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12, Object o13, Object o14, Object o15, Object o16, Object o17) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13, o14, o15, o16, o17);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12, Object o13, Object o14, Object o15, Object o16, Object o17, Object o18) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13, o14, o15, o16, o17, o18);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12, Object o13, Object o14, Object o15, Object o16, Object o17, Object o18, Object o19) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13, o14, o15, o16, o17, o18, o19);
  }

  @Override
  public Object invoke(Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9, Object o10, Object o11, Object o12, Object o13, Object o14, Object o15, Object o16, Object o17, Object o18, Object o19, Object... objects) {
    return impl.invoke(o, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13, o14, o15, o16, o17, o18, o19, objects);
  }

  @Override
  public Object applyTo(ISeq args) {
    return impl.applyTo(args);
  }

  @Override
  public void run() {
    impl.run();
  }

  @Override
  public Object call() throws Exception {
    return impl.call();
  }

  @Override
  public IObj withMeta(IPersistentMap meta) {
    if (impl instanceof IObj) {
      return ((IObj)impl).withMeta(meta);
    } else {
      return null;
    }
  }

  @Override
  public IPersistentMap meta() {
    if (impl instanceof IObj) {
      return ((IObj)impl).meta();
    } else {
      return null;
    }
  }
}
