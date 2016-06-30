package com.bloomberg.news.lucene.queryparser.xml.builders;

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

import org.apache.lucene.queryparser.xml.QueryBuilderFactory;
import org.apache.lucene.queryparser.xml.builders.MatchAllDocsQueryBuilder;
import org.apache.lucene.queryparser.xml.builders.TermQueryBuilder;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class TestDisjunctionMaxQueryBuilder extends LuceneTestCase {

  public void testDisjunctionMaxQuery() throws Exception {

    final QueryBuilderFactory queryFactory = new QueryBuilderFactory();
    queryFactory.addBuilder("TermQuery", new TermQueryBuilder());
    queryFactory.addBuilder("MatchAllDocsQuery", new MatchAllDocsQueryBuilder());
    final DisjunctionMaxQueryBuilder dmqBuilder = new DisjunctionMaxQueryBuilder(queryFactory);

    final float tieBreakerMultiplier = random().nextFloat();
    final boolean madQueryOnly = random().nextBoolean();
    final String xml = "<DisjunctionMaxQuery fieldName='content' tieBreaker='"+tieBreakerMultiplier+"'>"
        + (madQueryOnly ? "" : "<TermQuery fieldName='title'>guide</TermQuery>")
        + "<MatchAllDocsQuery/>"
        + "</DisjunctionMaxQuery>";
    final Document doc = getDocumentFromString(xml);

    final Query expectedQuery;
    if (madQueryOnly) {
      expectedQuery = new MatchAllDocsQuery();
    } else {
      final DisjunctionMaxQuery dmQuery = new DisjunctionMaxQuery(tieBreakerMultiplier);
      dmQuery.add(new TermQuery(new Term("title", "guide")));
      expectedQuery = dmQuery;
    }

    final Query actualQuery = dmqBuilder.getQuery(doc.getDocumentElement());
    assertEquals(expectedQuery, actualQuery);
  }

  // same as TestNumericRangeQueryBuilder's method of the same name
  private static Document getDocumentFromString(String str)
      throws SAXException, IOException, ParserConfigurationException {
    InputStream is = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(is);
    is.close();
    return doc;
  }

}
