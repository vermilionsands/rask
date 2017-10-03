package rask.api;

import clojure.lang.IFn;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import rask.util.SerializableVolatile;

public class SourceFn extends RaskFunction implements SourceFunction {

  private SerializableVolatile stopHandle = new SerializableVolatile(false);

  public SourceFn(IFn fn) {
    super(fn);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run(SourceContext sourceContext) throws Exception {
    fn.invoke(sourceContext, stopHandle);
  }

  @Override
  public void cancel() {
    stopHandle.reset(true);
  }
}
