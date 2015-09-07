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

import java.util.Collection;
import java.util.Set;

import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;

/**
 * Encapsulates the result of a {@link SearchGroupsFieldCommand} command
 */
public class SearchGroupsFieldCommandResult {

  private final Integer groupCount;
  private final Collection<SearchGroup<BytesRef>> searchGroups;
  private final Collection<SearchGroup<BytesRef>> groups;
  private final Set<BytesRef> excludedGroupValues;

  public SearchGroupsFieldCommandResult(Integer groupCount, Collection<SearchGroup<BytesRef>> searchGroups,
      Collection<SearchGroup<BytesRef>> groups,
      Set<BytesRef> excludedGroupValues) {
    this.groupCount = groupCount;
    this.searchGroups = searchGroups;
    this.groups = groups;
    this.excludedGroupValues = excludedGroupValues;
  }

  public Integer getGroupCount() {
    return groupCount;
  }

  public Collection<SearchGroup<BytesRef>> getSearchGroups() {
    return searchGroups;
  }

  public Collection<SearchGroup<BytesRef>> getGroups() {
    return groups;
  }

  public Set<BytesRef> getExcludedGroupValues() {
    return excludedGroupValues;
  }
}
