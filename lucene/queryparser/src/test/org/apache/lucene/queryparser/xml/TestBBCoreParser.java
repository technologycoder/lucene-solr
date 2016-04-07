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
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsFilter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.intervals.FieldedBooleanQuery;
import org.apache.lucene.search.intervals.IntervalFilterQuery;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.search.intervals.RangeIntervalFilter;
import org.apache.lucene.search.intervals.UnorderedNearQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


public class TestBBCoreParser extends LuceneTestCase {

  private static Analyzer analyzer;
  private static CoreParser bbCoreParser;
  private static Directory dir;
  private static IndexReader reader;
  private static IndexSearcher searcher;
  private static String ANALYSER_PARAM     = "tests.TestParser.analyser";
  private static String DEFAULT_ANALYSER   = "mock";
  private static String STANDARD_ANALYSER  = "standard";
  private static String KEYWORD_ANALYSER   = "keyword";
 
  @BeforeClass
  public static void beforeClass() throws Exception {
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
      analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
    }
    bbCoreParser = new BBCoreParser("contents", analyzer);
    
    //MatchAllDocsFilter is not yet in side the builderFactory
    //Remove this when we have MatchAllDocsFilter within CorePlusExtensionsParser
    bbCoreParser.filterFactory.addBuilder("MatchAllDocsFilter", new FilterBuilder() {
      
      @Override
      public Filter getFilter(Element e) throws ParserException {
        return new MatchAllDocsFilter();
      }
    });

    BufferedReader d = new BufferedReader(new InputStreamReader(
        TestParser.class.getResourceAsStream("reuters21578.txt"), StandardCharsets.US_ASCII));
    dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    String line = d.readLine();
    while (line != null) {
      int endOfDate = line.indexOf('\t');
      String date = line.substring(0, endOfDate).trim();
      String content = line.substring(endOfDate).trim();
      Document doc = new Document();
      doc.add(newTextField("date", date, Field.Store.YES));
      doc.add(newTextField("contents", content, Field.Store.YES));
      doc.add(new IntField("date2", Integer.valueOf(date), Field.Store.YES));
      writer.addDocument(doc);
      line = d.readLine();
    }
    d.close();
    writer.close();
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);

  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    dir.close();
    reader = null;
    searcher = null;
    dir = null;
    bbCoreParser = null;
  }

  public void testTermQueryXML() throws ParserException, IOException {
    Query q = parse("TermQuery.xml");
    dumpResults("TermQuery", q, 5);
  }
  
  public void testTermQueryEmptyXML() throws ParserException, IOException {
    parse("TermQueryEmpty.xml", true/*shouldFail*/);
  }
  
  public void testTermQueryStopwordXML() throws ParserException, IOException {
    parse("TermQueryStopwords.xml", true/*shouldFail*/);
  }
  
  public void testTermQueryMultipleTermsXML() throws ParserException, IOException {
    parse("TermQueryMultipleTerms.xml", true/*shouldFail*/);
  }

  public void testSimpleTermsQueryXML() throws ParserException, IOException {
    Query q = parse("TermsQuery.xml");
    assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    dumpResults("TermsQuery", q, 5);
  }

  public void testSimpleTermsQueryWithTermElementXML() throws ParserException, IOException {
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
  

  public void testBooleanQueryXML() throws ParserException, IOException {
    Query q = parse("BooleanQuery.xml");
    dumpResults("BooleanQuery", q, 5);
  }

  public void testDisjunctionMaxQueryXML() throws ParserException, IOException {
    Query q = parse("DisjunctionMaxQuery.xml");
    assertTrue(q instanceof DisjunctionMaxQuery);
    DisjunctionMaxQuery d = (DisjunctionMaxQuery)q;
    assertEquals(0.0f, d.getTieBreakerMultiplier(), 0.0001f);
    assertEquals(2, d.getDisjuncts().size());
    DisjunctionMaxQuery ndq = (DisjunctionMaxQuery) d.getDisjuncts().get(1);
    assertEquals(1.2f, ndq.getTieBreakerMultiplier(), 0.0001f);
    assertEquals(1, ndq.getDisjuncts().size());
  }

  public void testRangeFilterQueryXML() throws ParserException, IOException {
    Query q = parse("RangeFilterQuery.xml");
    dumpResults("RangeFilter", q, 5);
  }

  public void testUserQueryXML() throws ParserException, IOException {
    Query q = parse("UserInputQuery.xml");
    dumpResults("UserInput with Filter", q, 5);
  }

  public void testCustomFieldUserQueryXML() throws ParserException, IOException {
    Query q = parse("UserInputQueryCustomField.xml");
    int h = searcher.search(q, null, 1000).totalHits;
    assertEquals("UserInputQueryCustomField should produce 0 result ", 0, h);
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
  
  public void testBoostingTermQueryXML() throws Exception {
    Query q = parse("BoostingTermQuery.xml");
    dumpResults("BoostingTermQuery", q, 5);
  }

  public void testSpanTermXML() throws Exception {
    Query q = parse("SpanQuery.xml");
    dumpResults("Span Query", q, 5);
  }

  public void testConstantScoreQueryXML() throws Exception {
    Query q = parse("ConstantScoreQuery.xml");
    dumpResults("ConstantScoreQuery", q, 5);
  }
  
  public void testPhraseQueryXML() throws Exception {
    Query q = parse("PhraseQuery.xml");
    assertTrue("Expecting a PhraseQuery, but resulted in " + q.getClass(), q instanceof PhraseQuery);
    dumpResults("PhraseQuery", q, 5);
  }
  
  public void testPhraseQueryXMLWithStopwordsXML() throws Exception {
    if (analyzer() instanceof StandardAnalyzer) {
      parse("PhraseQueryStopwords.xml", true/*shouldfail*/);
    }
  }
  
  public void testPhraseQueryXMLWithNoTextXML() throws Exception {
    parse("PhraseQueryEmpty.xml", true/*shouldFail*/);
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

  public void testMatchAllDocsPlusFilterXML() throws ParserException, IOException {
    Query q = parse("MatchAllDocsQuery.xml");
    dumpResults("MatchAllDocsQuery with range filter", q, 5);
  }

  public void testBooleanFilterXML() throws ParserException, IOException {
    Query q = parse("BooleanFilter.xml");
    dumpResults("Boolean filter", q, 5);
  }

  public void testNestedBooleanQuery() throws ParserException, IOException {
    Query q = parse("NestedBooleanQuery.xml");
    dumpResults("Nested Boolean query", q, 5);
  }

  public void testCachedFilterXML() throws ParserException, IOException {
    Query q = parse("CachedFilter.xml");
    dumpResults("Cached filter", q, 5);
  }

  public void testNumericRangeFilterQueryXML() throws ParserException, IOException {
    Query q = parse("NumericRangeFilterQuery.xml");
    dumpResults("NumericRangeFilter", q, 5);
  }
  
  public void testNumericRangeQuery() throws IOException {
    String text = "<NumericRangeQuery fieldName='date2' lowerTerm='19870409' upperTerm='19870412'/>";
    Query q = parseText(text, false);
    dumpResults("NumericRangeQuery1", q, 5);
    text = "<NumericRangeQuery fieldName='date2' lowerTerm='19870602' />";
    q = parseText(text, false);
    dumpResults("NumericRangeQuery2", q, 5);
    text = "<NumericRangeQuery fieldName='date2' upperTerm='19870408'/>";
    q = parseText(text, false);
    dumpResults("NumericRangeQuery3", q, 5);
  }
  
  public void testNumericRangeFilter() throws IOException {
    String text = "<ConstantScoreQuery><NumericRangeFilter fieldName='date2' lowerTerm='19870410' upperTerm='19870531'/></ConstantScoreQuery>";
    Query q = parseText(text, false);
    dumpResults("NumericRangeFilter1", q, 5);
    text = "<ConstantScoreQuery><NumericRangeFilter fieldName='date2' lowerTerm='19870601' /></ConstantScoreQuery>";
    q = parseText(text, false);
    dumpResults("NumericRangeFilter2", q, 5);
    text = "<ConstantScoreQuery><NumericRangeFilter fieldName='date2' upperTerm='19870408'/></ConstantScoreQuery>";
    q = parseText(text, false);
    dumpResults("NumericRangeFilter3", q, 5);
  }
  
  public void testDisjunctionMaxQuery_MatchAllDocsQuery() throws IOException {
    String text = "<DisjunctionMaxQuery fieldName='content'>"
        + "<WildcardNearQuery>rio de janeiro</WildcardNearQuery>"
        + "<WildcardNearQuery>summit</WildcardNearQuery>"
        + "<WildcardNearQuery> </WildcardNearQuery></DisjunctionMaxQuery>";
    Query q = parseText(text, false);
    int size = ((DisjunctionMaxQuery)q).getDisjuncts().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    DisjunctionMaxQuery dm = (DisjunctionMaxQuery)q;
    for(Query q1 : dm.getDisjuncts())
    {
      assertFalse("Not expecting MatchAllDocsQuery ",q1 instanceof MatchAllDocsQuery);
    }
    
    text = "<DisjunctionMaxQuery fieldName='content' >"
        + "<MatchAllDocsQuery/>"
        + "<WildcardNearQuery> </WildcardNearQuery></DisjunctionMaxQuery>";
    q = parseText(text, false);
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);

  }
  
  //federal N/3 (credit OR (taxes N/1 income))
  public void testNearBooleanNear() throws IOException, ParserException {
    String text = ""
                  +"<NearQuery fieldName=\"contents\" slop=\"4\" inOrder=\"false\">"
                  +"<WildcardNearQuery>bank</WildcardNearQuery>"
                  +"<BooleanQuery disableCoord=\"true\"> "
                  +"<Clause occurs=\"should\"><TermQuery>quarter</TermQuery></Clause>"
                  +"<Clause occurs=\"should\">"
                  +"<NearQuery slop=\"2\" inOrder=\"false\">"
                  +"<WildcardNearQuery>earlier,</WildcardNearQuery>"
                  +"<WildcardNearQuery>april</WildcardNearQuery>"
                  +"</NearQuery>"
                  +"</Clause>"
                  +"</BooleanQuery>"
                  +"</NearQuery>"
                  ;
    Query q = parseText(text, false);
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
    String text = ""
                  +"<NearFirstQuery fieldName=\"contents\" end=\"5\">"
                  +"<BooleanQuery disableCoord=\"true\"> "
                  +"<Clause occurs=\"must\"><WildcardNearQuery>ban*</WildcardNearQuery></Clause>"
                  +"<Clause occurs=\"must\"><WildcardNearQuery>sa*</WildcardNearQuery></Clause>"
                  +"</BooleanQuery>"
                  +"</NearFirstQuery>"
                  ;
    Query q = parseText(text, false);
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
  
  public void testBooleanQuerywithMatchAllDocsQuery() throws IOException {
    String text = "<BooleanQuery fieldName='content' disableCoord='true'>"
        + "<Clause occurs='should'><WildcardNearQuery>rio de janeiro</WildcardNearQuery></Clause>"
        + "<Clause occurs='should'><WildcardNearQuery>summit</WildcardNearQuery></Clause>"
        + "<Clause occurs='should'><WildcardNearQuery> </WildcardNearQuery></Clause></BooleanQuery>";
    Query q = parseText(text, false);
    int size = ((BooleanQuery)q).clauses().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    BooleanQuery bq = (BooleanQuery)q;
    for(BooleanClause bc : bq.clauses())
    {
      assertFalse("Not expecting MatchAllDocsQuery ",bc.getQuery() instanceof MatchAllDocsQuery);
    }
  
    text = "<BooleanQuery fieldName='content' disableCoord='true'>"
        + "<Clause occurs='must'><WildcardNearQuery>rio de janeiro</WildcardNearQuery></Clause>"
        + "<Clause occurs='should'><WildcardNearQuery> </WildcardNearQuery></Clause></BooleanQuery>";
    q = parseText(text, false);
    assertTrue("Expecting a IntervalFilterQuery, but resulted in " + q.getClass(), q instanceof IntervalFilterQuery);
    
    text = "<BooleanQuery fieldName='content' disableCoord='true'>"
        + "<Clause occurs='must'><WildcardNearQuery>rio de janeiro</WildcardNearQuery></Clause>"
        + "<Clause occurs='must'><WildcardNearQuery>summit</WildcardNearQuery></Clause>"
        + "<Clause occurs='should'><WildcardNearQuery> </WildcardNearQuery></Clause></BooleanQuery>";
    q = parseText(text, false);
    assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    bq = (BooleanQuery)q;
    size = bq.clauses().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    for(BooleanClause bc : bq.clauses())
    {
      assertFalse("Not expecting MatchAllDocsQuery ", bc.getQuery() instanceof MatchAllDocsQuery);
    }
    
    text = "<BooleanQuery fieldName='content' disableCoord='true'>"
        + "<Clause occurs='must'><MatchAllDocsQuery/></Clause>"
        + "<Clause occurs='should'><WildcardNearQuery> </WildcardNearQuery></Clause></BooleanQuery>";
    q = parseText(text, false);
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    
    text = "<BooleanQuery fieldName='content' disableCoord='true'>"
        + "<Clause occurs='must'><MatchAllDocsQuery/></Clause>"
        + "<Clause occurs='mustnot'><TermQuery>summit</TermQuery></Clause></BooleanQuery>";
    q = parseText(text, false);
    assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    bq = (BooleanQuery)q;
    size = bq.clauses().size();
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
  
  public void testMaxBooleanCalusesWithPrefixQuery() throws ParserException, IOException {
    int maxClauseCount = BooleanQuery.getMaxClauseCount();
    BooleanQuery.setMaxClauseCount(3);
    String text = "<GenericTextQuery fieldName='contents' wnq='true'>inc*</GenericTextQuery>";
    Query q = parseText(text, false);
    dumpResults("testMaxBooleanCalusesWithPrefixQuery", q, 5);
    BooleanQuery.setMaxClauseCount(maxClauseCount);
  }

  //================= Helper methods ===================================

  protected Analyzer analyzer() {
    return analyzer;
  }

  protected CoreParser coreParser() {
    return bbCoreParser;
  }

  private Query parse(String xmlFileName) throws IOException {
    return parse(xmlFileName, false);
  }
  
  private Query parse(String xmlFileName, Boolean shouldFail) throws IOException {
    InputStream xmlStream = TestParser.class.getResourceAsStream(xmlFileName);
    assertTrue("Test XML file " + xmlFileName + " cannot be found", xmlStream != null);
    Query result = parse(xmlStream, shouldFail);
    xmlStream.close();
    return result;
  }
  private Query parseText(String text, Boolean shouldFail) 
  {
    InputStream xmlStream = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    return parse(xmlStream, shouldFail);
  }
  
  private Query parse(InputStream xmlStream, Boolean shouldFail)
  {
    Query result = null;
    try {
      result = coreParser().parse(xmlStream);
    } catch (ParserException ex) {
      assertTrue("Parser exception " + ex, shouldFail);
    }
    if (shouldFail && result != null)
      assertTrue("Expected to fail. But resulted in query: " + result.getClass() + " with value: " + result, false);
    return result;
  }

  private void dumpResults(String qType, Query q, int numDocs) throws IOException {
    if (VERBOSE) {
      System.out.println("=======TEST: " + q.getClass() + " query=" + q);
    }
    TopDocs hits = searcher.search(q, null, numDocs);
    assertTrue(qType + " " + q + " should produce results ", hits.totalHits > 0);
    if (true) {
      System.out.println("=========" + qType + " class=" + q.getClass() + " query=" + q + "============");
      ScoreDoc[] scoreDocs = hits.scoreDocs;
      for (int i = 0; i < Math.min(numDocs, hits.totalHits); i++) {
        Document ldoc = searcher.doc(scoreDocs[i].doc);
        System.out.println("[" + ldoc.get("date") + "]" + ldoc.get("contents"));
      }
      System.out.println();
    }
  }

  //helper
  private static Element parseXML(String text) throws ParserException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = null;
    try {
      db = dbf.newDocumentBuilder();
    }
    catch (Exception se) {
      throw new ParserException("XML Parser configuration error", se);
    }
    org.w3c.dom.Document doc = null;
    InputStream xmlStream = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    try {
      doc = db.parse(xmlStream);
    }
    catch (Exception se) {
      throw new ParserException("Error parsing XML stream:" + se, se);
    }
    return doc.getDocumentElement();
  }
}
