package org.apache.lucene.search.intervals;

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

/**
 * An IntervalFilter that restricts an IntervalIterator to return
 * only Intervals that occur in order within a given distance.
 *
 * @see WithinIntervalFilter
 */
public class WithinOrderedFilter implements IntervalFilter {

  private final WithinIntervalFilter innerFilter;
  private final boolean collectLeaves;
  private final String field;

  /**
   * Constructs a new WithinOrderedFilter with a given slop
   * @param slop The maximum distance allowed between subintervals
   * @param collectLeaves false if only the parent interval should be collected
   */
  public WithinOrderedFilter(String field, int slop, boolean collectLeaves) {
    this.innerFilter = new WithinIntervalFilter(slop);
    this.collectLeaves = collectLeaves;
    this.field = field;
  }

  public WithinOrderedFilter(String field, int slop) {
    this(field, slop, true);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((field == null) ? 0 : 2*field.hashCode() + (collectLeaves ? 1 : 0));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (getClass() != obj.getClass())
      return false;
    WithinOrderedFilter other = (WithinOrderedFilter) obj;
    if (collectLeaves != other.collectLeaves) {
      return false;
    }
    if (field != other.field) {
      return false;
    }
    if (innerFilter == null) {
      if (other.innerFilter != null)
        return false;
    } else if (!innerFilter.equals(other.innerFilter))
      return false;
    return true;
  }

  @Override
  public IntervalIterator filter(boolean collectIntervals, IntervalIterator iter) {
    return innerFilter.filter(collectIntervals,
                              new OrderedConjunctionIntervalIterator(collectIntervals, collectLeaves, field, iter));
  }

  @Override
  public String toString() {
    return "WithinOrderedFilter[" + this.innerFilter.getSlop() + "]";
  }

}
