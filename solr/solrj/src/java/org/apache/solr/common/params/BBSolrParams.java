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

package org.apache.solr.common.params;

import java.util.Iterator;
import java.util.List;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * Bloomberg-specific tweaks to the SolrParams class.
 */
public abstract class BBSolrParams {

  /* A variant of the SolrParams.toNamedList() method. */
  static public NamedList<Object> toNamedList(SolrParams solrParams, List<String> multi_valued_keys) {
    final SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();
    
    for(Iterator<String> it=solrParams.getParameterNamesIterator(); it.hasNext(); ) {
      final String name = it.next();
      final String [] values = solrParams.getParams(name);
      if (values.length==1 && (null == multi_valued_keys || !multi_valued_keys.contains(name))) {
        result.add(name,values[0]);
      } else {
        // currently no reason not to use the same array
        result.add(name,values);
      }
    }
    return result;
  }
}
