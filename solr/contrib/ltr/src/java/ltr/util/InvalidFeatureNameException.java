package ltr.util;

public class InvalidFeatureNameException extends LtrException {

	private static final long serialVersionUID = 1L;

	public InvalidFeatureNameException(String featureName) {
		super("Invalid feature name " + featureName);
	}

  public InvalidFeatureNameException(String featureName, Exception parent) {
    super("Invalid feature name " + featureName, parent);
  }
	
}
