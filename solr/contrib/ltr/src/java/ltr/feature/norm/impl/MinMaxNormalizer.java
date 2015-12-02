/**
 *
 */
package ltr.feature.norm.impl;

import ltr.feature.norm.Normalizer;
import ltr.util.NamedParams;
import ltr.util.NormalizerException;

public class MinMaxNormalizer extends Normalizer {

	private float min;
	private float max;
	private float delta;

	public void init(NamedParams params) throws NormalizerException {
		super.init(params);
		if (!params.containsKey("min"))
			throw new NormalizerException(
					"missing required param [min] for normalizer MinMaxNormalizer");
		if (!params.containsKey("max"))
			throw new NormalizerException(
					"missing required param [max] for normalizer MinMaxNormalizer");
		try {
			min = (float) params.getFloat("min");

			max = (float) params.getFloat("max");

		} catch (Exception e) {
			throw new NormalizerException(
					"invalid param value for normalizer MinMaxNormalizer", e);
		}

		delta = max - min;
		if (delta <= 0) {
			throw new NormalizerException(
					"invalid param value for MinMaxNormalizer, min must be lower than max ");
		}
	}

	@Override
	public float normalize(float value) {
		return (value - min) / delta;
	}

}
