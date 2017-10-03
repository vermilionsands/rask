package rask.api;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.MapFunction;

public class MapFn extends RaskFunction implements MapFunction {

  public MapFn(IFn fn) {
    super(fn);
  }

  @Override
  public Object map(Object o) throws Exception {
    return fn.invoke(o);
  }
}
