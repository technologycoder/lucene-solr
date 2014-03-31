package org.apache.lucene.queries;

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

import org.apache.lucene.search.IntegerRange;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermFreqQuery;
import org.apache.lucene.search.TermQuery;

/**
 * A filter that includes documents that match a specific term
 * <code>IntegerRange.min</code> to <code>IntegerRange.max</code> times.
 */
final public class TermFreqFilter extends QueryWrapperFilter {

  /**
   * Construct a <code>TermFreqFilter</code>.
   *
   * @param termFilter
   *          The TermFilter to which term frequency filtering should be applied.
   * @param termFreqRange
   *          The term frequency range filter to apply.
   */
  public TermFreqFilter(TermFilter termFilter, IntegerRange termFreqRange) {
    super(new TermFreqQuery((TermQuery)termFilter.getQuery(), termFreqRange));
  }
}
