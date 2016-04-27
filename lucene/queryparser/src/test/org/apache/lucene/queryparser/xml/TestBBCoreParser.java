package org.apache.lucene.queryparser.xml;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsFilter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.intervals.FieldedBooleanQuery;
import org.apache.lucene.search.intervals.IntervalFilterQuery;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.search.intervals.UnorderedNearQuery;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


public class TestBBCoreParser extends TestCoreParser {

  private static String ANALYSER_PARAM     = "tests.TestParser.analyser";
  private static String DEFAULT_ANALYSER   = "mock";
  private static String STANDARD_ANALYSER  = "standard";
  private static String KEYWORD_ANALYSER   = "keyword";
 
  @Override
  protected Analyzer newAnalyzer() {
    final Analyzer analyzer;
    String analyserToUse = System.getProperty(ANALYSER_PARAM, DEFAULT_ANALYSER);
    if (analyserToUse.equals(STANDARD_ANALYSER))
    {
      analyzer = new StandardAnalyzer(TEST_VERSION_CURRENT);
    }
    else if (analyserToUse.equals(KEYWORD_ANALYSER))
    {
      analyzer = new KeywordAnalyzer();
    }
    else
    {
      assertEquals(DEFAULT_ANALYSER, analyserToUse);
      // TODO: rewrite test (this needs to set QueryParser.enablePositionIncrements, too, for work with CURRENT):
      analyzer = super.newAnalyzer();
    }
    return analyzer;
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    final CoreParser bbCoreParser = new BBCoreParser(defaultField, analyzer);
    
    //MatchAllDocsFilter is not yet in side the builderFactory
    //Remove this when we have MatchAllDocsFilter within CorePlusExtensionsParser
    bbCoreParser.filterFactory.addBuilder("MatchAllDocsFilter", new FilterBuilder() {
      
      @Override
      public Filter getFilter(Element e) throws ParserException {
        return new MatchAllDocsFilter();
      }
    });

    return bbCoreParser;
  }

  public void testTermQueryStopwordXML() throws IOException {
    parseShouldFail("TermQueryStopwords.xml",
        "Empty term found. field:contents value:to a. Check the query analyzer configured on this field.");
  }
  
  public void testTermQueryMultipleTermsXML() throws IOException {
    parseShouldFail("TermQueryMultipleTerms.xml",
        "Multiple terms found. field:contents value:sumitomo come home. Check the query analyzer configured on this field.");
  }

  public void testTermsQueryShouldBeBooleanXML() throws ParserException, IOException {
    Query q = parse("TermsQuery.xml");
    assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    dumpResults("TermsQuery", q, 5);
  }

  public void testTermsQueryWithTermElementXML() throws ParserException, IOException {
    Query q = parse("TermsQueryWithTermElement.xml");
    dumpResults("TermsQuery", q, 5);
  }
  
  public void testTermsQueryWithSingleTerm() throws ParserException, IOException {
    Query q = parse("TermsQuerySingleTerm.xml");
    assertTrue("Expecting a TermQuery, but resulted in " + q.getClass(), q instanceof TermQuery);
    dumpResults("TermsQueryWithSingleTerm", q, 5);
  }
  
  
  //term appears like single term but results in two terms when it runs through standard analyzer
  public void testTermsQueryWithStopwords() throws ParserException, IOException {
    Query q = parse("TermsQueryStopwords.xml");
    if (analyzer() instanceof StandardAnalyzer)
      assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    dumpResults("TermsQueryWithStopwords", q, 5);
    }
  
  public void testTermsQueryEmpty() throws ParserException, IOException {
    Query q = parse("TermsQueryEmpty.xml");
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    dumpResults("Empty TermsQuery", q, 5);
  }
  
  public void testTermsQueryWithOnlyStopwords() throws ParserException, IOException {
    Query q = parse("TermsQueryOnlyStopwords.xml");
    if (analyzer() instanceof StandardAnalyzer)
      assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    dumpResults("TermsQuery with only stopwords", q, 5);
  }
  

  public void testTermsFilterXML() throws Exception {
    Query q = parse("TermsFilterQuery.xml");
    dumpResults("Terms Filter", q, 5);
  }
  
  public void testTermsFilterWithSingleTerm() throws Exception {
    Query q = parse("TermsFilterQueryWithSingleTerm.xml");
    dumpResults("TermsFilter With SingleTerm", q, 5);
  }
  
  public void testTermsFilterQueryWithStopword() throws Exception {
    Query q = parse("TermsFilterQueryStopwords.xml");
    dumpResults("TermsFilter with Stopword", q, 5);
  }
  
  public void testTermsFilterQueryWithOnlyStopword() throws Exception {
    Query q = parse("TermsFilterOnlyStopwords.xml");
    dumpResults("TermsFilter with all stop words", q, 5);
  }
  
  public void testBooleanFilterXML() throws ParserException, IOException {
    Query q = parse("BooleanFilter.xml");
    dumpResults("Boolean filter", q, 5);
  }

  public void testPhraseQueryXML() throws Exception {
    Query q = parse("PhraseQuery.xml");
    assertTrue("Expecting a PhraseQuery, but resulted in " + q.getClass(), q instanceof PhraseQuery);
    dumpResults("PhraseQuery", q, 5);
  }
  
  public void testPhraseQueryXMLWithStopwordsXML() throws Exception {
    if (analyzer() instanceof StandardAnalyzer) {
      parseShouldFail("PhraseQueryStopwords.xml",
          "Empty phrase query generated for field:contents, phrase:and to a");
    }
  }
  
  public void testPhraseQueryXMLWithNoTextXML() throws Exception {
    parseShouldFail("PhraseQueryEmpty.xml",
        "PhraseQuery has no text");
  }

  public void testGenericTextQueryXML() throws Exception {
    Query q = parse("GenericTextQuery.xml");
    assertTrue("Expecting a PhraseQuery, but resulted in " + q.getClass(), q instanceof PhraseQuery);
    dumpResults("GenericTextQuery", q, 5);
  }
  
  public void testGenericTextQuerySingleTermXML() throws Exception {
    Query q = parse("GenericTextQuerySingleTerm.xml");
    assertTrue("Expecting a TermQuery, but resulted in " + q.getClass(), q instanceof TermQuery);
    dumpResults("GenericTextQuery", q, 5);
  }
  
  public void testGenericTextQueryWithStopwordsXML() throws Exception {
    Query q = parse("GenericTextQueryStopwords.xml");
    assertTrue("Expecting a PhraseQuery, but resulted in " + q.getClass(), q instanceof PhraseQuery);
    dumpResults("GenericTextQuery with stopwords", q, 5);
  }
  
  public void testGenericTextQueryWithAllStopwordsXML() throws Exception {
    Query q = parse("GenericTextQueryAllStopwords.xml");
    if (analyzer() instanceof StandardAnalyzer)
      assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    dumpResults("GenericTextQuery with just stopwords", q, 5);
  }
  
  public void testGenericTextQueryWithNoTextXML() throws Exception {
    Query q = parse("GenericTextQueryEmpty.xml");
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    dumpResults("GenericTextQuery with no text", q, 5);
  }
  
  public void testGenericTextQueryPhraseWildcardXML() throws Exception {
    Query q = parse("GenericTextQueryPhraseWildcard.xml");
    dumpResults("GenericTextQuery with a phrase wildcard", q, 5);
  }
  
  public void testGenericTextQueryTrailingWildcardXML() throws Exception {
    Query q = parse("GenericTextQueryTrailingWildcard.xml");
    dumpResults("GenericTextQuery with a trailing wildcard", q, 5);
  }

  public void testGenericTextQueryMultiWildcardXML() throws Exception {
    Query q = parse("GenericTextQueryMultiWildcard.xml");
    dumpResults("GenericTextQuery with multiple terms containing wildcards", q, 5);
  }
  
  public void testGenericTextQueryPhraseWildcard2XML() throws Exception {
    Query q = parse("GenericTextQueryPhraseWildcard2.xml");
    dumpResults("GenericTextQuery with a phrase wildcard", q, 5);
  }
  
  public void testGenericTextQueryTrailingWildcard2XML() throws Exception {
    Query q = parse("GenericTextQueryTrailingWildcard2.xml");
    dumpResults("GenericTextQuery with a trailing wildcard", q, 5);
  }

  public void testGenericTextQueryMultiWildcard2XML() throws Exception {
    Query q = parse("GenericTextQueryMultiWildcard2.xml");
    dumpResults("GenericTextQuery with multiple terms containing wildcards", q, 5);
  }

  public void testGenericTextQueryMultiClauseXML() throws Exception {
    Query q = parse("GenericTextQueryMultiClause.xml");
    dumpResults("GenericTextQuery. BooleanQuery containing multiple GenericTextQuery clauses with different boost factors", q, 5);
  }

  public void testDisjunctionMaxQueryTripleWildcardNearQuery() throws Exception {
    Query q = parse("DisjunctionMaxQueryTripleWildcardNearQuery.xml");
    int size = ((DisjunctionMaxQuery)q).getDisjuncts().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    DisjunctionMaxQuery dm = (DisjunctionMaxQuery)q;
    for(Query q1 : dm.getDisjuncts())
    {
      assertFalse("Not expecting MatchAllDocsQuery ",q1 instanceof MatchAllDocsQuery);
    }
  }

  public void testDisjunctionMaxQueryMatchAllDocsQuery() throws Exception {
    final Query q = parse("DisjunctionMaxQueryMatchAllDocsQuery.xml");
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
  }
  
  public void testNearBooleanNear() throws IOException, ParserException {
    final Query q = parse("NearBooleanNear.xml");
    dumpResults("testNearBooleanNear", q, 5);
  }
  
  
  //working version of (A OR B) N/5 C
  public void testNearBoolean() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("contents", "iranian")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("contents", "north")), BooleanClause.Occur.SHOULD);
    
    FieldedQuery[] subQueries = new FieldedQuery[2];
    subQueries[0] = FieldedBooleanQuery.toFieldedQuery(bq);
    subQueries[1] = FieldedBooleanQuery.toFieldedQuery(new TermQuery(new Term("contents", "akbar")));
    FieldedQuery fq = new UnorderedNearQuery(5, subQueries);
    dumpResults("testNearBoolean", fq, 5);
  }
  
  public void testNearFirstBooleanMustXml() throws IOException, ParserException {
    final Query q = parse("NearFirstBooleanMust.xml");
    dumpResults("testNearFirstBooleanMustXml", q, 50);
  }
  
  public void testNearFirstBooleanMust() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("contents", "upholds")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("contents", "building")), BooleanClause.Occur.MUST);
    
    FieldedQuery[] subQueries = new FieldedQuery[2];
    subQueries[0] = FieldedBooleanQuery.toFieldedQuery(bq);
    subQueries[1] = FieldedBooleanQuery.toFieldedQuery(new TermQuery(new Term("contents", "bank")));
    FieldedQuery fq = new UnorderedNearQuery(7, subQueries);
    dumpResults("testNearFirstBooleanMust", fq, 5);
  }
  
  public void testBooleanQueryTripleShouldWildcardNearQuery() throws Exception {
    final Query q = parse("BooleanQueryTripleShouldWildcardNearQuery.xml");
    final int size = ((BooleanQuery)q).clauses().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    final BooleanQuery bq = (BooleanQuery)q;
    for(BooleanClause bc : bq.clauses())
    {
      assertFalse("Not expecting MatchAllDocsQuery ",bc.getQuery() instanceof MatchAllDocsQuery);
    }
  }
  
  public void testBooleanQueryMustShouldWildcardNearQuery() throws Exception {
    final Query q = parse("BooleanQueryMustShouldWildcardNearQuery.xml");
    assertTrue("Expecting a IntervalFilterQuery, but resulted in " + q.getClass(), q instanceof IntervalFilterQuery);
  }

  public void testBooleanQueryMustMustShouldWildcardNearQuery() throws Exception {
    final Query q = parse("BooleanQueryMustMustShouldWildcardNearQuery.xml");
    assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    final BooleanQuery bq = (BooleanQuery)q;
    final int size = bq.clauses().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    for(BooleanClause bc : bq.clauses())
    {
      assertFalse("Not expecting MatchAllDocsQuery ", bc.getQuery() instanceof MatchAllDocsQuery);
    }
  }

  public void testBooleanQueryMatchAllDocsQueryWildcardNearQuery() throws Exception {
    final Query q = parse("BooleanQueryMatchAllDocsQueryWildcardNearQuery.xml");
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
  }

  public void testBooleanQueryMatchAllDocsQueryTermQuery() throws Exception {
    final Query q = parse("BooleanQueryMatchAllDocsQueryTermQuery.xml");
    assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    final BooleanQuery bq = (BooleanQuery)q;
    final int size = bq.clauses().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    boolean bMatchAllDocsFound = false;
    for(BooleanClause bc : bq.clauses())
    {
      bMatchAllDocsFound |= bc.getQuery() instanceof MatchAllDocsQuery;
    }
    assertTrue("Expecting MatchAllDocsQuery ", bMatchAllDocsFound);
  }
  
  public void testBooleanFilterwithMatchAllDocsFilter() throws ParserException, IOException {
    
    String text = "<BooleanFilter fieldName='content' disableCoord='true'>"
        + "<Clause occurs='should'><TermFilter>janeiro</TermFilter></Clause>"
        + "<Clause occurs='should'><MatchAllDocsFilter/></Clause></BooleanFilter>";
    
    Filter f = coreParser().filterFactory.getFilter(parseXML(text));
    assertTrue("Expecting a TermFilter, but resulted in " + f.getClass(), f instanceof TermFilter);
  
    text = "<BooleanFilter fieldName='content' disableCoord='true'>"
        + "<Clause occurs='must'><TermFilter>rio</TermFilter></Clause>"
        + "<Clause occurs='should'><MatchAllDocsFilter/></Clause></BooleanFilter>";
    f = coreParser().filterFactory.getFilter(parseXML(text));
    assertTrue("Expecting a TermFilter, but resulted in " + f.getClass(), f instanceof TermFilter);
    
    text = "<BooleanFilter fieldName='content' disableCoord='true'>"
        + "<Clause occurs='must'><TermFilter>rio</TermFilter></Clause>"
        + "<Clause occurs='must'><TermFilter>janeiro</TermFilter></Clause>"
        + "<Clause occurs='must'><TermFilter>summit</TermFilter></Clause>"
        + "<Clause occurs='should'><MatchAllDocsFilter/></Clause></BooleanFilter>";
    f = coreParser().filterFactory.getFilter(parseXML(text));
    assertTrue("Expecting a BooleanFilter, but resulted in " + f.getClass(), f instanceof BooleanFilter);
    BooleanFilter bf = (BooleanFilter)f;
    int size = bf.clauses().size();
    assertTrue("Expecting 3 clauses, but resulted in " + size, size == 3);
    for(FilterClause fc : bf.clauses())
    {
      assertFalse("Not expecting MatchAllDocsQuery ", fc.getFilter() instanceof MatchAllDocsFilter);
    }
    
    text = "<BooleanFilter fieldName='content' disableCoord='true'>"
        + "<Clause occurs='must'><MatchAllDocsFilter/></Clause>"
        + "<Clause occurs='should'><MatchAllDocsFilter/></Clause></BooleanFilter>";
    f = coreParser().filterFactory.getFilter(parseXML(text));
    assertTrue("Expecting a MatchAllDocsFilter, but resulted in " + f.getClass(), f instanceof MatchAllDocsFilter);
    
    text = "<BooleanFilter fieldName='content' disableCoord='true'>"
        + "<Clause occurs='must'><MatchAllDocsFilter/></Clause>"
        + "<Clause occurs='mustnot'><TermFilter>summit</TermFilter></Clause></BooleanFilter>";
    f = coreParser().filterFactory.getFilter(parseXML(text));
    assertTrue("Expecting a BooleanFilter, but resulted in " + f.getClass(), f instanceof BooleanFilter);
    bf = (BooleanFilter)f;
    size = bf.clauses().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    boolean bMatchAllDocsFound = false;
    for(FilterClause fc : bf.clauses())
    {
      bMatchAllDocsFound |= fc.getFilter() instanceof MatchAllDocsFilter;
    }
    assertTrue("Expecting MatchAllDocsFilter ", bMatchAllDocsFound);
    
  }
  
  public void testNearTermQuery() throws ParserException, IOException {
    int slop = 1;
    FieldedQuery[] subqueries = new FieldedQuery[2];
    subqueries[0] = new TermQuery(new Term("contents", "keihanshin"));
    subqueries[1] = new TermQuery(new Term("contents", "real"));
    Query q = new OrderedNearQuery(slop, true, subqueries);
    dumpResults("NearPrefixQuery", q, 5);
  }
  
  public void testPrefixedNearQuery() throws ParserException, IOException {
    int slop = 1;
    FieldedQuery[] subqueries = new FieldedQuery[2];
    subqueries[0] = new PrefixQuery(new Term("contents", "keihanshi"));
    ((MultiTermQuery)subqueries[0]).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    subqueries[1] = new PrefixQuery(new Term("contents", "rea"));
    ((MultiTermQuery)subqueries[1]).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    Query q = new OrderedNearQuery(slop, true, subqueries);
    dumpResults("NearPrefixQuery", q, 5);
  }
  
  public void testGenericTextQueryMaxBooleanClausesWithPrefixQuery() throws ParserException, IOException {
    final int maxClauseCount = BooleanQuery.getMaxClauseCount();
    try {
      BooleanQuery.setMaxClauseCount(3);
      final Query q = parse("GenericTextQueryMaxBooleanClausesWithPrefixQuery.xml");
      dumpResults("GenericTextQueryMaxBooleanClausesWithPrefixQuery", q, 5);
    } finally {
      BooleanQuery.setMaxClauseCount(maxClauseCount);
    }
  }

  //================= Helper methods ===================================

  private static Element parseXML(String text) throws ParserException {
    InputStream xmlStream = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    org.w3c.dom.Document doc = CoreParser.parseXML(xmlStream);
    return doc.getDocumentElement();
  }
}
