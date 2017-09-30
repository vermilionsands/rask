package rask.util;

import clojure.lang.IDeref;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class SerializableVolatile implements IDeref, Serializable {
  volatile Object val;

  public SerializableVolatile(Object val) {
    this.val = val;
  }

  public Object deref() {
    return this.val;
  }

  public Object reset(Object newval) {
    return this.val = newval;
  }

  // required to have the correct instances of true/false
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    if (val instanceof Boolean) {
      val = Boolean.valueOf((Boolean)val);
    }
  }

}
