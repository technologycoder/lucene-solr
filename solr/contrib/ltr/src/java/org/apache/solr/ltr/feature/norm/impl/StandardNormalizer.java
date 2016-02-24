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

public class StandardNormalizer extends Normalizer {

  private static final String AVG_KEY = "avg";
  private static final String STD_KEY = "std";

  private static final float AVG_VALUE_DEFAULT = 0.0f;
  private static final float STD_VALUE_DEFAULT = 1.0f;

  private float avg = AVG_VALUE_DEFAULT;
  private float std = STD_VALUE_DEFAULT;

  public void init(NamedParams params) throws NormalizerException {
    super.init(params);
    if (!params.containsKey(AVG_KEY)) {
      throw new NormalizerException("missing param ["+AVG_KEY+"]");
    }
    if (!params.containsKey(STD_KEY)) {
      throw new NormalizerException("missing param ["+STD_KEY+"]");
    }
    avg = params.getFloat(AVG_KEY, AVG_VALUE_DEFAULT);
    std = params.getFloat(STD_KEY, STD_VALUE_DEFAULT);
    if (std <= 0.0f) throw new NormalizerException(STD_KEY+" must be > 0");
  }

  @Override
  public float normalize(float value) {
    return (value - avg) / std;
  }

  @Override
  public String toString() {
    return type + " ["
      +AVG_KEY+"="+avg+","
      +STD_KEY+"="+std+"]";
  }
}
