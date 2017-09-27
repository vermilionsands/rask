package rask.api;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.FoldFunction;

public class FoldFn extends RequiringFunction implements FoldFunction {

  public FoldFn(IFn fn) {
    super(fn);
  }

  @Override
  public Object fold(Object acc, Object x) throws Exception {
    return fn.invoke(acc, x);
  }
}
