package rask.deprecated.timestamps;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import rask.deprecated.util.SerializableFn;

public class AscendingTimestampFn extends AscendingTimestampExtractor{

  private final SerializableFn fn;

  public AscendingTimestampFn(SerializableFn fn) {
    super();
    this.fn = fn;
  }

  public long extractAscendingTimestamp(Object o) {
    return (long)fn.invoke(o);
  }
}
