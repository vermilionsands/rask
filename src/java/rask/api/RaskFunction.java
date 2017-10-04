package rask.api;

import clojure.lang.*;
import clojure.lang.Compiler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public abstract class RaskFunction implements Serializable {

  public static String CORE_NS = "clojure.core";
  public static String RASK_NS = "rask.api";
  public static Var REQUIRE = RT.var(CORE_NS, "require");
  public static Var FIND_NS = RT.var(CORE_NS, "find-ns");
  public static Keyword FN_NAME_KEY = Keyword.intern(RASK_NS, "name");
  public static Keyword FORM_KEY = Keyword.intern(RASK_NS, "form");

  private Symbol sym = null;
  private PersistentList form = null;

  protected transient IFn fn = null;

  private static synchronized void optionalRequire(Symbol ns) {
    if (FIND_NS.invoke(ns) == null) {
      REQUIRE.invoke(ns);
    }
  }

  public static IFn initFn(Symbol fn, PersistentList form) {
    IFn fnImpl = null;
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
      fnImpl = (IFn)Compiler.eval(form);
      Var.popThreadBindings();
    }
    return fnImpl;
  }

  public RaskFunction(IFn fn) {
    IPersistentMap meta = RT.meta(fn);
    if (meta == null) meta = PersistentHashMap.EMPTY;
    this.sym = (Symbol)RT.get(meta, FN_NAME_KEY);
    this.form = (PersistentList)RT.get(meta, FORM_KEY);
    this.fn = fn;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.fn = initFn(sym, form);
  }
}
