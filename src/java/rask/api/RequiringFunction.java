package rask.api;

import clojure.lang.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public abstract class RequiringFunction implements Serializable {

  private static Var REQUIRE_VAR = RT.var("clojure.core", "require");
  private static Var FIND_NS = RT.var("clojure.core", "find-ns");
  private static Keyword NS_KEY = Keyword.intern("rask.api", "namespace");
  private Symbol implNs = null;
  IFn fn = null;

  public RequiringFunction(IFn fn) {
    this.fn = fn;
    Symbol s = getImplNs(fn);
    if (s != null) {
      implNs = s;
    }
  }

  private Symbol getImplNs(IFn x) {
    IPersistentMap meta = RT.meta(x);
    return (meta != null) ? (Symbol)RT.get(meta, NS_KEY) : null;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    if (implNs != null && FIND_NS.invoke(implNs) == null) {
      REQUIRE_VAR.invoke(implNs);
    }
  }
}
