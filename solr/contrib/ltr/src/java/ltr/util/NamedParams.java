package ltr.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamedParams extends HashMap<String,Object> {

  private static final long serialVersionUID = 1L;
  public static final NamedParams EMPTY = new NamedParams();

  public static float convertToFloat(Object o) {
    float f = 0;
    if (o instanceof Double) {
      double d = (Double) o;
      f = (float) d;
      return f;
    }
    if (o instanceof Integer) {
      int d = (Integer) o;
      f = (float) d;
      return f;
    }
    if (o instanceof Long) {
      long l = (Long) o;
      f = (float) l;
      return f;
    }
    if (o instanceof Float) {
      Float ff = (Float) o;
      f = (float) ff;
      return f;
    }

    throw new NumberFormatException(o.getClass().getName() + " cannot be converted to float");
  }

  public NamedParams() {
  }

  public NamedParams(Map<String,Object> params) {
    for (Map.Entry<String,Object> p : params.entrySet()) {
      add(p.getKey(), p.getValue());
    }
  }

  @SuppressWarnings("unchecked")
  public NamedParams(Object o) {
    this((Map<String,Object>) o);
  }

  public NamedParams add(String name, Object value) {
    put(name, value);
    return this;
  }

  public double getDouble(String key, double defValue) {
    if (containsKey(key)) {
      return (double) get(key);
    }
    return defValue;
  }

  public List<Object> getList(String key) {
    if (containsKey(key)) {
      return (List<Object>) get(key);
    }
    return null;
  }

  public float getFloat(String key) {
    Object o = get(key);
    return convertToFloat(o);
  }

  public float getFloat(String key, float value) {
    return (containsKey(key)) ? getFloat(key) : value;
  }

}
