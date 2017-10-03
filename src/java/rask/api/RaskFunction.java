package rask.api;

import clojure.lang.*;
import clojure.lang.Compiler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public abstract class RaskFunction implements Serializable {

  private static String CORE_NS = "clojure.core";
  private static String RASK_NS = "rask.api";
  private static Var REQUIRE = RT.var(CORE_NS, "require");
  private static Var FIND_NS = RT.var(CORE_NS, "find-ns");
  private static Keyword FN_NAME_KEY = Keyword.intern(RASK_NS, "name");
  private static Keyword FORM_KEY = Keyword.intern(RASK_NS, "form");

  private Symbol sym = null;
  private PersistentList form = null;

  transient IFn fn = null;

  public RaskFunction(IFn fn) {
    IPersistentMap meta = RT.meta(fn);
    if (meta == null) meta = PersistentHashMap.EMPTY;
    this.sym = (Symbol)RT.get(meta, FN_NAME_KEY);
    this.form = (PersistentList)RT.get(meta, FORM_KEY);
    this.fn = fn;
  }

  private void initFn(Symbol fn) {
    Symbol ns = Symbol.intern(fn.getNamespace());
    if (FIND_NS.invoke(ns) == null) {
      REQUIRE.invoke(ns);
    }
    Var x = RT.var(fn.getNamespace(), fn.getName());
    if (x.isBound()) {
      this.fn = x;
    } else {
      Namespace nsObject = (Namespace)FIND_NS.invoke(ns);
      Var.pushThreadBindings(
          PersistentHashMap.create(
              Var.intern(Symbol.intern(CORE_NS), Symbol.intern("*ns*")).setDynamic(),
              nsObject));
      this.fn = (IFn)Compiler.eval(form);
      Var.popThreadBindings();
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initFn(sym);
  }
}
