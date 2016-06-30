package org.apache.solr.search;

import java.io.InputStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.BBCoreParser;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.Query;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.xml.BoostedQueryBuilder;
import org.apache.solr.search.xml.RangeFilterBuilder;
import org.apache.solr.search.xml.RangeQueryBuilder;
import org.apache.solr.search.xml.WildcardQueryBuilder;
import org.w3c.dom.Element;

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
 * Assembles a QueryBuilder which uses Query objects from Solr's <code>search</code> module
 * in addition to Query objects supported by the passed in Lucene <code>CoreParser</code>.
 */
public class BBSolrCoreParser extends BBCoreParser {

  public BBSolrCoreParser(String defaultField, Analyzer analyzer,
      SolrQueryRequest req) {
    super(defaultField, analyzer);

    final IndexSchema schema = req.getSchema();

    queryFactory.addBuilder("RangeQuery", new RangeQueryBuilder(schema));
    filterFactory.addBuilder("RangeFilter", new RangeFilterBuilder(schema));

    queryFactory.addBuilder("WildcardQuery", new WildcardQueryBuilder(schema));

    queryFactory.addBuilder("BoostedQuery", new BoostedQueryBuilder(this, req));
  }

}
