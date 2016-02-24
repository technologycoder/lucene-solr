package org.apache.solr.ltr.feature.norm.impl;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.ltr.util.NormalizerException;

public class MinMaxNormalizer extends Normalizer {

  private static final String MIN_KEY = "min";
  private static final String MAX_KEY = "max";

  private static final float MIN_VALUE_DEFAULT = Float.MIN_VALUE;
  private static final float MAX_VALUE_DEFAULT = Float.MAX_VALUE;

  private float min = MIN_VALUE_DEFAULT;
  private float max = MAX_VALUE_DEFAULT;
  private float delta = max - min;

  public void init(NamedParams params) throws NormalizerException {
    super.init(params);
    if (!params.containsKey(MIN_KEY)) throw new NormalizerException(
        "missing required param ["+MIN_KEY+"] for normalizer MinMaxNormalizer");
    if (!params.containsKey(MAX_KEY)) throw new NormalizerException(
        "missing required param ["+MAX_KEY+"] for normalizer MinMaxNormalizer");
    try {
      min = (float) params.getFloat(MIN_KEY, MIN_VALUE_DEFAULT);

      max = (float) params.getFloat(MAX_KEY, MAX_VALUE_DEFAULT);

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

  @Override
  public String toString() {
    return type + " ["
      +MIN_KEY+"="+min+","
      +MAX_KEY+"="+max+"]";
  }
}
