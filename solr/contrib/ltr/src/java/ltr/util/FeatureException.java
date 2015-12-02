package ltr.util;

public class FeatureException extends LtrException {

  private static final long serialVersionUID = 1L;

  public FeatureException(String msg) {
    super(msg);
  }

  public FeatureException(String msg, Exception parent) {
    super(msg, parent);
  }

}
