package rask.api;

import clojure.lang.IFn;
import org.apache.flink.api.java.functions.KeySelector;

public class KeySelectorFn extends RequiringFunction implements KeySelector{

  public KeySelectorFn(IFn fn) {
    super(fn);
  }

  @Override
  public Object getKey(Object o) throws Exception {
    return fn.invoke(o);
  }
}
