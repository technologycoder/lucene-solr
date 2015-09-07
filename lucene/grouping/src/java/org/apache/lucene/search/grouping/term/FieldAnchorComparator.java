package org.apache.lucene.search.grouping.term;

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

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.AnchorComparator;

/*
 * A simple wrapper around a FieldComparator which allows use
 * only of methods that don't modify the slot value(s). And
 * which also takes care of reverse sort order logic.
 */
public class FieldAnchorComparator<ANCHOR_VALUE_TYPE> implements AnchorComparator {

  private final FieldComparator<?> in;
  private final int reversed;

  public static FieldAnchorComparator<?> create(Sort sort, Object anchorValue) throws IOException {
    final SortField[] sortFields = sort.getSort();
    if (sortFields.length <= 0) {
      throw new IllegalArgumentException("sortFields[0] must exist and be assignable from "+anchorValue.getClass()+" anchorValue="+anchorValue);
    }
    return new FieldAnchorComparator<>(sortFields[0], anchorValue);
  }

  @SuppressWarnings("unchecked")
  public FieldAnchorComparator(SortField sortField, ANCHOR_VALUE_TYPE anchorValue) throws IOException {
    in = sortField.getComparator(1, 0);
    if (!in.value(0).getClass().isAssignableFrom(anchorValue.getClass())) {
      throw new IllegalArgumentException("sortField="+sortField+" must be assignable from "+anchorValue.getClass()+" anchorValue="+anchorValue);
    } else {
      ((FieldComparator<ANCHOR_VALUE_TYPE>)in).setTopValue(anchorValue);
    }
    reversed = sortField.getReverse() ? -1 : 1;
  }

  @Override
  public int compare(int doc) throws IOException {
    return in.compareTop(doc)*reversed;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    in.setNextReader(readerContext);
  }

  @Override
  public void setScorer(Scorer scorer) {
    in.setScorer(scorer);
  }

}
