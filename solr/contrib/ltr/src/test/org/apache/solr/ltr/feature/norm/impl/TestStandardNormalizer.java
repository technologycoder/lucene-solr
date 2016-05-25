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

public class TestStandardNormalizer {

  private final SolrResourceLoader solrResourceLoader = new SolrResourceLoader();

  @Test(expected = NormalizerException.class)
  public void testNormalizerNoParams() throws NormalizerException {
    Normalizer.getInstance(
        StandardNormalizer.class.getCanonicalName(), new NamedParams(), solrResourceLoader);

  }

  @Test(expected = NormalizerException.class)
  public void testInvalidSTD() throws NormalizerException {

    Normalizer.getInstance(
        StandardNormalizer.class.getCanonicalName(),
        new NamedParams().add("std", 0f), solrResourceLoader);

  }

  @Test(expected = NormalizerException.class)
  public void testInvalidSTD2() throws NormalizerException {

    Normalizer.getInstance(
        StandardNormalizer.class.getCanonicalName(),
        new NamedParams().add("std", -1f), solrResourceLoader);

  }

  @Test(expected = NormalizerException.class)
  public void testInvalidSTD3() throws NormalizerException {
    Normalizer.getInstance(
        StandardNormalizer.class.getCanonicalName(),
        new NamedParams().add("avg", 1f).add("std", 0f), solrResourceLoader);
  }

  @Test
  public void testNormalizer() throws NormalizerException {
    final Normalizer identity = Normalizer.getInstance(
        StandardNormalizer.class.getCanonicalName(),
        new NamedParams().add("avg", 0f).add("std", 1f), solrResourceLoader);

    float value = 8;
    assertEquals(value, identity.normalize(value), 0.0001);
    value = 150;
    assertEquals(value, identity.normalize(value), 0.0001);
    final Normalizer norm = Normalizer.getInstance(
        StandardNormalizer.class.getCanonicalName(),
        new NamedParams().add("avg", 10f).add("std", 1.5f), solrResourceLoader);

    for (final float v : new float[] {10f, 20f, 25f, 30f, 31f, 40f, 42f, 100f,
        10000000f}) {
      assertEquals((v - 10f) / (1.5f), norm.normalize(v), 0.0001);
    }
  }
}
