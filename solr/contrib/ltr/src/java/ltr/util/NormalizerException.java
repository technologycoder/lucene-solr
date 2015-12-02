/**
 *
 */
package ltr.util;

public class NormalizerException extends LtrException {

	private static final long serialVersionUID = 1L;

  public NormalizerException(String msg) {
    super(msg);
  }

  public NormalizerException(String message, Exception parent) {
    super(message, parent);
  }

}
