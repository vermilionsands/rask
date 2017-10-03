package rask.api;

import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class FlatMapFn extends RaskFunction implements FlatMapFunction {

  public FlatMapFn(IFn fn) {
    super(fn);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void flatMap(Object o, Collector collector) throws Exception {
    Iterator i = RT.iter(fn.invoke(o));
    while(i.hasNext()) {
      collector.collect(i.next());
    }
  }
}
