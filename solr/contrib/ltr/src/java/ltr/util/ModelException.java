package ltr.util;

public class ModelException extends LtrException {

  private static final long serialVersionUID = 1L;

  public ModelException(String message) {
    super(message);
  }

  public ModelException(String message, Exception parent) {
    super(message, parent);
  }

}
