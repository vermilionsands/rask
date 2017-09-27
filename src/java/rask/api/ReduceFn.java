package rask.api;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ReduceFn extends RequiringFunction implements ReduceFunction {

  public ReduceFn(IFn fn) {
    super(fn);
  }

  @Override
  public Object reduce(Object acc, Object x) throws Exception {
    return fn.invoke(acc, x);
  }
}
