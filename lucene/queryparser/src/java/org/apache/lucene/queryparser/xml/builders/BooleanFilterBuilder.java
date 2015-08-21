/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsFilter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
 * Builder for {@link BooleanFilter}
 */
public class BooleanFilterBuilder implements FilterBuilder {

  private final FilterBuilder factory;

  public BooleanFilterBuilder(FilterBuilder factory) {
    this.factory = factory;
  }

  @Override
  public Filter getFilter(Element e) throws ParserException {
    BooleanFilter bf = new BooleanFilter();

    boolean matchAllDocsExists = false; 
    boolean shouldOrMustExists = false;
    
    NodeList nl = e.getChildNodes();
    final int nl_len = nl.getLength();
    for (int i = 0; i < nl_len; i++) {
      Node node = nl.item(i);
      if (node.getNodeName().equals("Clause")) {
        Element clauseElem = (Element) node;
        BooleanClause.Occur occurs = BooleanQueryBuilder.getOccursValue(clauseElem);

        Element clauseFilter = DOMUtils.getFirstChildOrFail(clauseElem);
        Filter f = factory.getFilter(clauseFilter);
        
        //MatchAllDocs needs to be added back only if there is no other should or must clause and there is no need to have duplicates of them.
        if (f instanceof MatchAllDocsFilter) {
          matchAllDocsExists = true;
          continue;
        }
        else if ((occurs == BooleanClause.Occur.SHOULD) || (occurs == BooleanClause.Occur.MUST)){
          shouldOrMustExists = true;
        }
        bf.add(new FilterClause(f, occurs));
      }
    }
    //MatchallDocs query needs to be added only if there is no other must or should clauses in the query.
    //At least we preserve the users intention to execute the rest of the query. instead of flooding him with all the documents.
    if (matchAllDocsExists && !shouldOrMustExists) {  
      bf.add(new FilterClause(new MatchAllDocsFilter(), BooleanClause.Occur.MUST));
    }
    if(bf.clauses().size() == 1)
      return bf.clauses().get(0).getFilter();
    else

    return bf;
  }

}
