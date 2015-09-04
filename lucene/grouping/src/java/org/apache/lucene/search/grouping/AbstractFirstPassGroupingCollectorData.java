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

/** AbstractFirstPassGroupingCollectorData abstractly represents the data
 *  used by a AbstractFirstPassGroupingCollector.
 *
 * @lucene.experimental
 */
public class AbstractFirstPassGroupingCollectorData<GROUP_VALUE_TYPE> extends Collector {

  private final FieldComparator<?>[] comparators;
  private final int[] reversed;
  private final int topNGroups;
  private final HashMap<GROUP_VALUE_TYPE, CollectedSearchGroup<GROUP_VALUE_TYPE>> groupMap;
  private final int compIDXEnd;

  private final Set<GROUP_VALUE_TYPE> uncollectedGroups;

  // Set once we reach topNGroups unique groups:
  /** @lucene.internal */
  TreeSet<CollectedSearchGroup<GROUP_VALUE_TYPE>> orderedGroups;
  private int docBase;
  private int spareSlot;

  public AbstractFirstPassGroupingCollectorData(Sort groupSort, int topNGroups) throws IOException {
    this(groupSort, topNGroups, false);
  }
  @SuppressWarnings({"unchecked","rawtypes"})
  public AbstractFirstPassGroupingCollectorData(Sort groupSort, int topNGroups, boolean supportUncollect) throws IOException {
    if (topNGroups < 1) {
      throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
    }

    // TODO: allow null groupSort to mean "by relevance",
    // and specialize it?

    this.topNGroups = topNGroups;

    final SortField[] sortFields = groupSort.getSort();
    comparators = new FieldComparator[sortFields.length];
    compIDXEnd = comparators.length - 1;
    reversed = new int[sortFields.length];
    for (int i = 0; i < sortFields.length; i++) {
      final SortField sortField = sortFields[i];

      // use topNGroups + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot):
      comparators[i] = sortField.getComparator(topNGroups + 1, i);
      reversed[i] = sortField.getReverse() ? -1 : 1;
    }

    spareSlot = topNGroups;
    groupMap = new HashMap<>(topNGroups);

    if (supportUncollect) {
      uncollectedGroups = new HashSet<GROUP_VALUE_TYPE>();
    } else {
      uncollectedGroups = null;
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

    //System.out.println("FP.getTopGroups groupOffset=" + groupOffset + " fillFields=" + fillFields + " groupMap.size()=" + groupMap.size());

    if (groupOffset < 0) {
      throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
    }

    if (groupMap.size() <= groupOffset) {
      return null;
    }

    if (orderedGroups == null) {
      buildSortedSet();
    }

    final Collection<SearchGroup<GROUP_VALUE_TYPE>> result = new ArrayList<>();
    int upto = 0;
    final int sortFieldCount = comparators.length;
    for(CollectedSearchGroup<GROUP_VALUE_TYPE> group : orderedGroups) {
      if (uncollectedGroups != null && uncollectedGroups.contains(group.groupValue)) {
        continue;
      }
      if (upto++ < groupOffset) {
        continue;
      }
      //System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
      SearchGroup<GROUP_VALUE_TYPE> searchGroup = new SearchGroup<>();
      searchGroup.groupValue = group.groupValue;
      if (fillFields) {
        searchGroup.sortValues = new Object[sortFieldCount];
        for(int sortFieldIDX=0;sortFieldIDX<sortFieldCount;sortFieldIDX++) {
          searchGroup.sortValues[sortFieldIDX] = comparators[sortFieldIDX].value(group.comparatorSlot);
        }
      }
      result.add(searchGroup);
    }
    //System.out.println("  return " + result.size() + " groups");
    return result;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    for (FieldComparator<?> comparator : comparators) {
      comparator.setScorer(scorer);
    }
  }

  public void uncollect(int doc, AbstractFirstPassGroupingCollectorDataSource<GROUP_VALUE_TYPE> dataSource) throws IOException {
    if (uncollectedGroups == null) {
      throw new UnsupportedOperationException("AbstractFirstPassGroupingCollectorData.uncollect(doc="+doc+")");
    } else {
      final GROUP_VALUE_TYPE groupValue = dataSource.getDocGroupValue(doc);
      final CollectedSearchGroup<GROUP_VALUE_TYPE> group = groupMap.get(groupValue);
      if (group != null) {
        uncollectedGroups.add(dataSource.copyDocGroupValue(groupValue, null));
      }
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    throw new UnsupportedOperationException("AbstractFirstPassGroupingCollectorData.collect(doc="+doc+")");
  }

  public void collect(int doc, AbstractFirstPassGroupingCollectorDataSource<GROUP_VALUE_TYPE> dataSource) throws IOException {
    //System.out.println("FP.collect doc=" + doc);

    final GROUP_VALUE_TYPE groupValue;
    final CollectedSearchGroup<GROUP_VALUE_TYPE> group;

    // we have groups whose places in the map and tree we wish to recycle
    if (uncollectedGroups != null && !uncollectedGroups.isEmpty()) {
      groupValue = dataSource.getDocGroupValue(doc);
      group = groupMap.get(groupValue);
      if (group == null) { // this is a new group ...
        // ... that should displace an existing uncollected group
        final Iterator<GROUP_VALUE_TYPE> uncollectedGroupIterator = uncollectedGroups.iterator();

        final CollectedSearchGroup<GROUP_VALUE_TYPE> removedGroup = groupMap.remove(uncollectedGroupIterator.next());
        assert removedGroup != null;

        final CollectedSearchGroup<GROUP_VALUE_TYPE> oldLast;
        if (orderedGroups != null) {
          oldLast = orderedGroups.last();
          final boolean removed = orderedGroups.remove(removedGroup.groupValue);
          assert removed == true;
        } else {
          oldLast = null;
        }

        // reuse the removed CollectedSearchGroup
        removedGroup.groupValue = dataSource.copyDocGroupValue(groupValue, removedGroup.groupValue);
        removedGroup.topDoc = docBase + doc;

        for (FieldComparator<?> fc : comparators) {
          fc.copy(removedGroup.comparatorSlot, doc);
        }

        groupMap.put(removedGroup.groupValue, removedGroup);

        if (orderedGroups != null) {
          final CollectedSearchGroup<?> interimLast = orderedGroups.isEmpty() ? null : orderedGroups.last();
          final boolean added = orderedGroups.add(removedGroup);
          assert added == true;
          final CollectedSearchGroup<?> newLast = orderedGroups.last();
          if (newLast != oldLast || interimLast != oldLast) {
            for (FieldComparator<?> fc : comparators) {
              fc.setBottom(newLast.comparatorSlot);
            }
          }
        }

        uncollectedGroupIterator.remove();
        return;
      } else { // there is an existing group ...
        // ... so proceed to consider this new doc with respect to the existing group
      }
    }
    // If orderedGroups != null we already have collected N groups and
    // can short circuit by comparing this document to the bottom group,
    // without having to find what group this document belongs to.
    
    // Even if this document belongs to a group in the top N, we'll know that
    // we don't have to update that group.

    // Downside: if the number of unique groups is very low, this is
    // wasted effort as we will most likely be updating an existing group.
    else if (orderedGroups != null) {
      for (int compIDX = 0;; compIDX++) {
        final int c = reversed[compIDX] * comparators[compIDX].compareBottom(doc);
        if (c < 0) {
          // Definitely not competitive. So don't even bother to continue
          return;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (compIDX == compIDXEnd) {
          // Here c=0. If we're at the last comparator, this doc is not
          // competitive, since docs are visited in doc Id order, which means
          // this doc cannot compete with any other document in the queue.
          return;
        }
      }
      groupValue = dataSource.getDocGroupValue(doc);
      group = groupMap.get(groupValue);
    } else {
      groupValue = dataSource.getDocGroupValue(doc);
      group = groupMap.get(groupValue);
    }

    // TODO: should we add option to mean "ignore docs that
    // don't have the group field" (instead of stuffing them
    // under null group)?

    if (group == null) {

      // First time we are seeing this group, or, we've seen
      // it before but it fell out of the top N and is now
      // coming back

      if (groupMap.size() < topNGroups) {

        // Still in startup transient: we have not
        // seen enough unique groups to start pruning them;
        // just keep collecting them

        // Add a new CollectedSearchGroup:
        CollectedSearchGroup<GROUP_VALUE_TYPE> sg = new CollectedSearchGroup<>();
        sg.groupValue = dataSource.copyDocGroupValue(groupValue, null);
        sg.comparatorSlot = groupMap.size();
        sg.topDoc = docBase + doc;
        for (FieldComparator<?> fc : comparators) {
          fc.copy(sg.comparatorSlot, doc);
        }
        groupMap.put(sg.groupValue, sg);

        if (groupMap.size() == topNGroups) {
          // End of startup transient: we now have max
          // number of groups; from here on we will drop
          // bottom group when we insert new one:
          buildSortedSet();
        }

        return;
      }

      // We already tested that the document is competitive, so replace
      // the bottom group with this new group.
      final CollectedSearchGroup<GROUP_VALUE_TYPE> removedGroup = orderedGroups.pollLast();
      assert orderedGroups.size() == topNGroups -1;

      groupMap.remove(removedGroup.groupValue);

      // reuse the removed CollectedSearchGroup
      removedGroup.groupValue = dataSource.copyDocGroupValue(groupValue, removedGroup.groupValue);
      removedGroup.topDoc = docBase + doc;

      for (FieldComparator<?> fc : comparators) {
        fc.copy(removedGroup.comparatorSlot, doc);
      }

      groupMap.put(removedGroup.groupValue, removedGroup);
      orderedGroups.add(removedGroup);
      assert orderedGroups.size() == topNGroups;

      final int lastComparatorSlot = orderedGroups.last().comparatorSlot;
      for (FieldComparator<?> fc : comparators) {
        fc.setBottom(lastComparatorSlot);
      }

      return;
    }

    // Update existing group:
    for (int compIDX = 0;; compIDX++) {
      final FieldComparator<?> fc = comparators[compIDX];
      fc.copy(spareSlot, doc);

      final int c = reversed[compIDX] * fc.compare(group.comparatorSlot, spareSlot);
      if (c < 0) {
        // Definitely not competitive.
        return;
      } else if (c > 0) {
        // Definitely competitive; set remaining comparators:
        for (int compIDX2=compIDX+1; compIDX2<comparators.length; compIDX2++) {
          comparators[compIDX2].copy(spareSlot, doc);
        }
        break;
      } else if (compIDX == compIDXEnd) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }

    // Remove before updating the group since lookup is done via comparators
    // TODO: optimize this

    final CollectedSearchGroup<GROUP_VALUE_TYPE> prevLast;
    if (orderedGroups != null) {
      prevLast = orderedGroups.last();
      orderedGroups.remove(group);
      assert orderedGroups.size() == topNGroups-1;
    } else {
      prevLast = null;
    }

    group.topDoc = docBase + doc;

    // Swap slots
    final int tmp = spareSlot;
    spareSlot = group.comparatorSlot;
    group.comparatorSlot = tmp;

    // Re-add the changed group
    if (orderedGroups != null) {
      orderedGroups.add(group);
      assert orderedGroups.size() == topNGroups;
      final CollectedSearchGroup<?> newLast = orderedGroups.last();
      // If we changed the value of the last group, or changed which group was last, then update bottom:
      if (group == newLast || prevLast != newLast) {
        for (FieldComparator<?> fc : comparators) {
          fc.setBottom(newLast.comparatorSlot);
        }
      }
    }
  }

  private void buildSortedSet() {
    final Comparator<CollectedSearchGroup<?>> comparator = new Comparator<CollectedSearchGroup<?>>() {
      @Override
      public int compare(CollectedSearchGroup<?> o1, CollectedSearchGroup<?> o2) {
        for (int compIDX = 0;; compIDX++) {
          FieldComparator<?> fc = comparators[compIDX];
          final int c = reversed[compIDX] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);
          if (c != 0) {
            return c;
          } else if (compIDX == compIDXEnd) {
            return o1.topDoc - o2.topDoc;
          }
        }
      }
    };

    orderedGroups = new TreeSet<>(comparator);
    orderedGroups.addAll(groupMap.values());
    assert orderedGroups.size() > 0;

    for (FieldComparator<?> fc : comparators) {
      fc.setBottom(orderedGroups.last().comparatorSlot);
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    docBase = readerContext.docBase;
    for (int i=0; i<comparators.length; i++) {
      comparators[i] = comparators[i].setNextReader(readerContext);
    }
  }

}

