package rask.api.timestamps;

import clojure.lang.*;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import rask.api.RaskFunction;

import java.io.IOException;
import java.io.ObjectInputStream;

public class AscendingTimestampFn extends AscendingTimestampExtractor{

  private Symbol sym = null;
  private PersistentList form = null;

  protected transient IFn fn = null;

  public AscendingTimestampFn(IFn fn) {
    super();
    IPersistentMap meta = RT.meta(fn);
    if (meta == null) meta = PersistentHashMap.EMPTY;
    this.sym = (Symbol)RT.get(meta, RaskFunction.FN_NAME_KEY);
    this.form = (PersistentList)RT.get(meta, RaskFunction.FORM_KEY);
    this.fn = fn;
  }

  public long extractAscendingTimestamp(Object o) {
    return (long)fn.invoke(o);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.fn = RaskFunction.initFn(sym, form);
  }
}
