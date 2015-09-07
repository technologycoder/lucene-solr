package org.apache.solr.search.grouping.distributed.command;

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

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.term.FieldAnchorComparator;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.grouping.Command;

import java.io.IOException;
import java.util.*;

/**
 * Creates all the collectors needed for the first phase and how to handle the results.
 */
public class SearchGroupsFieldCommand implements Command<SearchGroupsFieldCommandResult> {

  public static class Builder {

    private SchemaField field;
    private Sort groupSort;
    private Integer topNGroups;
    private boolean includeGroupCount = false;
    private boolean anchorForward = true;
    private int aboveAnchorCount;
    private Object anchorValue = null;
    private int belowAnchorCount;

    public Builder setField(SchemaField field) {
      this.field = field;
      return this;
    }

    public Builder setGroupSort(Sort groupSort) {
      this.groupSort = groupSort;
      return this;
    }

    public Builder setTopNGroups(int topNGroups) {
      this.topNGroups = topNGroups;
      return this;
    }

    public Builder setIncludeGroupCount(boolean includeGroupCount) {
      this.includeGroupCount = includeGroupCount;
      return this;
    }

    public Builder setAnchorForward(boolean anchorForward) {
      this.anchorForward = anchorForward;
      return this;
    }

    public Builder setAboveAnchorCount(int aboveAnchorCount) {
      this.aboveAnchorCount = aboveAnchorCount;
      return this;
    }

    public Builder setAnchorValue(Object anchorValue) {
      this.anchorValue = anchorValue;
      return this;
    }

    public Builder setBelowAnchorCount(int belowAnchorCount) {
      this.belowAnchorCount = belowAnchorCount;
      return this;
    }

    public SearchGroupsFieldCommand build() {
      if (field == null || groupSort == null || topNGroups == null) {
        throw new IllegalStateException("All fields must be set");
      }

      return new SearchGroupsFieldCommand(field, groupSort, topNGroups, includeGroupCount, anchorForward, aboveAnchorCount, anchorValue, belowAnchorCount);
    }

  }

  private final SchemaField field;
  private final Sort groupSort;
  private final int topNGroups;
  private final boolean includeGroupCount;
  private boolean anchorForward = true;
  private final int aboveAnchorCount;
  private final Object anchorValue;
  private final int belowAnchorCount;

  private TermFirstPassGroupingCollector firstPassGroupingCollector;
  private TermAllGroupsCollector allGroupsCollector;

  private SearchGroupsFieldCommand(SchemaField field, Sort groupSort, int topNGroups, boolean includeGroupCount,
      boolean anchorForward, int aboveAnchorCount, Object anchorValue, int belowAnchorCount) {
    this.field = field;
    this.groupSort = groupSort;
    this.topNGroups = topNGroups;
    this.includeGroupCount = includeGroupCount;
    this.anchorForward = anchorForward;
    this.aboveAnchorCount = aboveAnchorCount;
    this.anchorValue = anchorValue;
    this.belowAnchorCount = belowAnchorCount;
  }

  @Override
  public List<Collector> create() throws IOException {
    final List<Collector> collectors = new ArrayList<>(2);
    if (topNGroups > 0) {
      if (anchorValue == null) {
        firstPassGroupingCollector = new TermFirstPassGroupingCollector(
            field.getName(), groupSort, topNGroups);
      } else {
        firstPassGroupingCollector = new TermFirstPassGroupingCollector(
            field.getName(), groupSort, topNGroups,
            anchorForward,
            aboveAnchorCount,
            FieldAnchorComparator.create(groupSort, anchorValue),
            belowAnchorCount);
      }
      collectors.add(firstPassGroupingCollector);
    }
    if (includeGroupCount) {
      allGroupsCollector = new TermAllGroupsCollector(field.getName());
      collectors.add(allGroupsCollector);
    }
    return collectors;
  }

  @Override
  public SearchGroupsFieldCommandResult result() {
    final Collection<SearchGroup<BytesRef>> topGroups;
    final Collection<SearchGroup<BytesRef>> groups;
    final Set<BytesRef> excludedGroupValues;
    if (firstPassGroupingCollector != null) {
      if (anchorValue == null) {
        topGroups = firstPassGroupingCollector.getTopGroups(0, true);
        groups = null;
        excludedGroupValues = null;
      } else {
        topGroups = null;
        groups = firstPassGroupingCollector.getGroups(0, true);
        excludedGroupValues = firstPassGroupingCollector.getExcludedGroupValues();
      }
    } else {
      topGroups = Collections.emptyList();
      groups = null;
      excludedGroupValues = null;
    }
    final Integer groupCount;
    if (allGroupsCollector != null) {
      groupCount = allGroupsCollector.getGroupCount();
    } else {
      groupCount = null;
    }
    return new SearchGroupsFieldCommandResult(groupCount, topGroups, groups, excludedGroupValues);
  }

  @Override
  public Sort getSortWithinGroup() {
    return null;
  }

  @Override
  public Sort getGroupSort() {
    return groupSort;
  }

  @Override
  public String getKey() {
    return field.getName();
  }
}
