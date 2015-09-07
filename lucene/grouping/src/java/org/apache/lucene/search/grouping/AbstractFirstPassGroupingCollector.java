package org.apache.lucene.search.grouping;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.*;

/** FirstPassGroupingCollector is the first of two passes necessary
 *  to collect grouped hits.  This pass gathers the top N sorted
 *  groups. Concrete subclasses define what a group is and how it
 *  is internally collected.
 *
 *  <p>See {@link org.apache.lucene.search.grouping} for more
 *  details including a full code example.</p>
 *
 * @lucene.experimental
 */
abstract public class AbstractFirstPassGroupingCollector<GROUP_VALUE_TYPE> extends Collector {

  private class AbstractFirstPassGroupingCollectorAsDataSource<GROUP_VALUE_TYPE> implements AbstractFirstPassGroupingCollectorDataSource<GROUP_VALUE_TYPE> {
    private final AbstractFirstPassGroupingCollector<GROUP_VALUE_TYPE> ds;
    AbstractFirstPassGroupingCollectorAsDataSource(AbstractFirstPassGroupingCollector<GROUP_VALUE_TYPE> ds) {
      this.ds = ds;
    }
    public GROUP_VALUE_TYPE getDocGroupValue(int doc) {
      return ds.getDocGroupValue(doc);
    }
    public GROUP_VALUE_TYPE copyDocGroupValue(GROUP_VALUE_TYPE groupValue, GROUP_VALUE_TYPE reuse) {
      return ds.copyDocGroupValue(groupValue, reuse);
    }
  }
  private final AbstractFirstPassGroupingCollectorAsDataSource<GROUP_VALUE_TYPE> thisAsDataSource;

  protected final AbstractFirstPassGroupingCollectorData<GROUP_VALUE_TYPE> data;
  private final boolean forward;
  private final AbstractFirstPassGroupingCollectorData<GROUP_VALUE_TYPE> aboveAnchorData;
  private final AnchorComparator anchor;
  private final AbstractFirstPassGroupingCollectorData<GROUP_VALUE_TYPE> belowAnchorData;

  /**
   * Create the first pass collector.
   *
   *  @param groupSort The {@link Sort} used to sort the
   *    groups.  The top sorted document within each group
   *    according to groupSort, determines how that group
   *    sorts against other groups.  This must be non-null,
   *    ie, if you want to groupSort by relevance use
   *    Sort.RELEVANCE.
   *  @param topNGroups How many top groups to keep.
   *  @throws IOException If I/O related errors occur
   */
  public AbstractFirstPassGroupingCollector(Sort groupSort, int topNGroups) throws IOException {
    this(groupSort, topNGroups, true, 0, null, 0);
  }
  /**
   * Create the first pass collector.
   *
   *  @param groupSort The {@link Sort} used to sort the
   *    groups.  The top sorted document within each group
   *    according to groupSort, determines how that group
   *    sorts against other groups.  This must be non-null,
   *    ie, if you want to groupSort by relevance use
   *    Sort.RELEVANCE.
   *  @param topNGroups How many top groups to keep.
   *  @param forward Direction i.e. true for forward/top results and false for backward/tail results
   *  @param aboveAnchorNGroups How many extra above-the-anchor groups to keep
   *  @param anchor Threshold delineating top and above-the-anchor groups
   *  @param belowAnchorNGroups How many extra below-the-anchor groups to keep
   *  @throws IOException If I/O related errors occur
   */
  public AbstractFirstPassGroupingCollector(Sort groupSort, int topNGroups,
      boolean forward, int aboveAnchorNGroups, AnchorComparator anchor, int belowAnchorNGroups) throws IOException {
    this.thisAsDataSource = new AbstractFirstPassGroupingCollectorAsDataSource<GROUP_VALUE_TYPE>(this);
    this.forward = forward;
    this.anchor = anchor;
    if (anchor == null) {
      if (forward) {
        this.data = new AbstractFirstPassGroupingCollectorData<GROUP_VALUE_TYPE>(groupSort, topNGroups, false, true);
        this.aboveAnchorData = null;
        this.belowAnchorData = null;
      } else {
        throw new IllegalArgumentException("not-forward direction requires an anchor");
      }
    } else {
      if (forward) {
        this.data = null;
        this.aboveAnchorData = new AbstractFirstPassGroupingCollectorData<GROUP_VALUE_TYPE>(groupSort, aboveAnchorNGroups, false, false);
        this.belowAnchorData = new AbstractFirstPassGroupingCollectorData<GROUP_VALUE_TYPE>(groupSort, topNGroups + belowAnchorNGroups, true, true);
      } else {
        this.data = null;
        this.aboveAnchorData = new AbstractFirstPassGroupingCollectorData<GROUP_VALUE_TYPE>(groupSort, topNGroups + aboveAnchorNGroups, false, false);
        this.belowAnchorData = null;
      }
    }
  }

  /**
   * Returns top groups, starting from offset.  This may
   * return null, if no groups were collected, or if the
   * number of unique groups collected is <= offset.
   *
   * @param groupOffset The offset in the collected groups
   * @param fillFields Whether to fill to {@link SearchGroup#sortValues}
   * @return top groups, starting from offset
   */
  public Collection<SearchGroup<GROUP_VALUE_TYPE>> getTopGroups(int groupOffset, boolean fillFields) {
    return (data == null ? null : data.getTopGroups(groupOffset, fillFields));
  }

  public Set<GROUP_VALUE_TYPE> getExcludedGroupValues() {
    if (forward) {
      return (aboveAnchorData == null ? null : aboveAnchorData.getGroupValues());
    } else {
      return (belowAnchorData == null ? null : belowAnchorData.getGroupValues());
    }
  }

  public Collection<SearchGroup<GROUP_VALUE_TYPE>> getGroups(int groupOffset, boolean fillFields) {
    if (forward) {
      return (belowAnchorData == null ? null : belowAnchorData.getTopGroups(groupOffset, fillFields));
    } else {
      return (aboveAnchorData == null ? null : aboveAnchorData.getTopGroups(groupOffset, fillFields));
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    if (data != null) data.setScorer(scorer);
    if (aboveAnchorData != null) aboveAnchorData.setScorer(scorer);
    if (anchor != null) anchor.setScorer(scorer);
    if (belowAnchorData != null) belowAnchorData.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    if (data != null) {
      data.collect(doc, thisAsDataSource);
    }

    if (anchor != null) {
      final int cc = anchor.compare(doc);
      if (cc < 0) { // < means 'anchor < doc' i.e. below anchor
        if (belowAnchorData != null) {
          belowAnchorData.collect(doc, thisAsDataSource);
        }
      } else if (cc > 0) { // > means 'anchor > doc' i.e. 'doc < anchor' i.e. above anchor
        if (aboveAnchorData != null) {
          aboveAnchorData.collect(doc, thisAsDataSource);
        }
        if (belowAnchorData != null) {
          // groups bubble to the top i.e. if the group is now above the anchor
          // then any document in the group below the anchor must be uncollected
          belowAnchorData.uncollect(doc, thisAsDataSource);
        }
      } else { // == means 'anchor == doc' i.e. below anchor
        if (belowAnchorData != null) {
          belowAnchorData.collect(doc, thisAsDataSource);
        } else if (aboveAnchorData != null) {
          aboveAnchorData.collect(doc, thisAsDataSource);
        }
      }
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    if (data != null) data.setNextReader(readerContext);
    if (aboveAnchorData != null) aboveAnchorData.setNextReader(readerContext);
    if (anchor != null) anchor.setNextReader(readerContext);
    if (belowAnchorData != null) belowAnchorData.setNextReader(readerContext);
  }

  /**
   * Returns the group value for the specified doc.
   *
   * @param doc The specified doc
   * @return the group value for the specified doc
   */
  protected abstract GROUP_VALUE_TYPE getDocGroupValue(int doc);

  /**
   * Returns a copy of the specified group value by creating a new instance and copying the value from the specified
   * groupValue in the new instance. Or optionally the reuse argument can be used to copy the group value in.
   *
   * @param groupValue The group value to copy
   * @param reuse Optionally a reuse instance to prevent a new instance creation
   * @return a copy of the specified group value
   */
  protected abstract GROUP_VALUE_TYPE copyDocGroupValue(GROUP_VALUE_TYPE groupValue, GROUP_VALUE_TYPE reuse);

}

