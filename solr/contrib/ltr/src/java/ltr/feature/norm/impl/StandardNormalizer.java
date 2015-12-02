/**
 *
 */
package ltr.feature.norm.impl;

import ltr.feature.norm.Normalizer;
import ltr.util.NamedParams;
import ltr.util.NormalizerException;

public class StandardNormalizer extends Normalizer {

  private float avg;
  private float std;

  public void init(NamedParams params) throws NormalizerException {
    super.init(params);
    if (!params.containsKey("avg")) {
      throw new NormalizerException("missing param avg");
    }
    if (!params.containsKey("std")) {
      throw new NormalizerException("missing param std");
    }
    avg = params.getFloat("avg", 0);
    std = params.getFloat("std", 1);
    if (std <= 0)
      throw new NormalizerException("std must be > 0");
  }

  @Override
  public float normalize(float value) {
    return (value - avg) / std;
  }

}
