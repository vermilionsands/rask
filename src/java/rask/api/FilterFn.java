package rask.api;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.FilterFunction;

public class FilterFn extends RaskFunction implements FilterFunction {

  public FilterFn(IFn fn) {
    super(fn);
  }

  @Override
  public boolean filter(Object o) throws Exception {
    return (boolean)fn.invoke(o);
  }
}
