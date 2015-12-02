package ltr.util;

import java.io.IOException;

public class LtrException extends IOException {

  private static final long serialVersionUID = 1L;

  public LtrException(String message) {
    super(message);
  }

  public LtrException(String message, Exception parent) {
    super(message, parent);
  }

}
