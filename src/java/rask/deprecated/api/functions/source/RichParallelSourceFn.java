package rask.deprecated.api.functions.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import rask.deprecated.util.SerializableFn;
import rask.deprecated.util.SerializableVolatile;

public class RichParallelSourceFn extends RichParallelSourceFunction{

  private SerializableFn fn;
  private SerializableVolatile handle;

  public RichParallelSourceFn(SerializableFn fn) {
    super();
    this.fn = fn;
    this.handle = new SerializableVolatile(false);
  }

  @Override
  public void run(SourceContext sourceContext) throws Exception {
    fn.invoke(sourceContext, handle);
  }

  @Override
  public void cancel() {
    handle.reset(true);
  }
}
