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

import static org.junit.Assert.assertEquals;

import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.ltr.util.NormalizerException;
import org.junit.Test;

public class TestMinMaxNormalizer {

  private final SolrResourceLoader solrResourceLoader = new SolrResourceLoader();

  @Test(expected = NormalizerException.class)
  public void testInvalidMinMaxNoParams() throws NormalizerException {
    Normalizer.getInstance(
        MinMaxNormalizer.class.getCanonicalName(), new NamedParams(), solrResourceLoader);

  }

  @Test(expected = NormalizerException.class)
  public void testInvalidMinMaxMissingMax() throws NormalizerException {

    Normalizer.getInstance(
        MinMaxNormalizer.class.getCanonicalName(),
        new NamedParams().add("min", "0.0f"), solrResourceLoader);

  }

  @Test(expected = NormalizerException.class)
  public void testInvalidMinMaxMissingMin() throws NormalizerException {

    Normalizer.getInstance(
        MinMaxNormalizer.class.getCanonicalName(),
        new NamedParams().add("max", "0.0f"), solrResourceLoader);

  }

  @Test(expected = NormalizerException.class)
  public void testMinMaxNormalizerMinLargerThanMax() throws NormalizerException {
    Normalizer.getInstance(
        MinMaxNormalizer.class.getCanonicalName(),
        new NamedParams().add("max", "0.0f").add("min", "10.0f"), solrResourceLoader);
  }

  @Test(expected = NormalizerException.class)
  public void testMinMaxNormalizerMinEqualToMax()
      throws NormalizerException {

    Normalizer.getInstance(
        "org.apache.solr.ltr.feature.norm.impl.MinMaxNormalizer",
        new NamedParams().add("min", "10.0f").add("max", "10.0f"), solrResourceLoader);
    // min == max
  }

  @Test
  public void testNormalizer() throws NormalizerException {
    final Normalizer n = Normalizer.getInstance(
        MinMaxNormalizer.class.getCanonicalName(),
        new NamedParams().add("min", "5.0f").add("max", "10.0f"), solrResourceLoader);

    float value = 8;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
    value = 100;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
    value = 150;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
    value = -1;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
    value = 5;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
  }
}
