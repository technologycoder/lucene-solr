package ltr.feature.norm.impl;

import ltr.feature.norm.Normalizer;
import ltr.util.NamedParams;

public class IdentityNormalizer extends Normalizer {

	public static final IdentityNormalizer INSTANCE = new IdentityNormalizer();

	public IdentityNormalizer() {

	}

	public void init(NamedParams params) {
	}

	@Override
	public float normalize(float value) {
		return value;
	}

}
