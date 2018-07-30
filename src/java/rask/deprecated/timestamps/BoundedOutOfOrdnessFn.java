package rask.deprecated.timestamps;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import rask.deprecated.util.SerializableFn;

public class BoundedOutOfOrdnessFn extends BoundedOutOfOrdernessTimestampExtractor {

  private final SerializableFn fn;

  public BoundedOutOfOrdnessFn(SerializableFn fn, Time maxOutOfOrderness) {
    super(maxOutOfOrderness);
    this.fn = fn;
  }

  @Override
  public long extractTimestamp(Object o) {
    return (long)fn.invoke(o);
  }
}
