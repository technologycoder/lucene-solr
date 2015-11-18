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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.ja.JapaneseBaseFormFilter;
import org.apache.lucene.analysis.ja.JapaneseKatakanaStemFilter;
import org.apache.lucene.analysis.ja.JapaneseNumberFilter;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.standard.BBFinancialStandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.builders.WildcardNearQueryParser;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Reader;

public class TestWildParser extends LuceneTestCase {
  private final class BBFinancialStandardAnalyzer extends Analyzer {
    // This is an approximation of an Analyzer that is actually built in Solr
    // using a config file.
    public BBFinancialStandardAnalyzer() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
      final BBFinancialStandardTokenizer src = new BBFinancialStandardTokenizer(TEST_VERSION_CURRENT, reader);
      TokenStream tok = new LowerCaseFilter(TEST_VERSION_CURRENT, src);
      return new TokenStreamComponents(src, tok);
    }
  }

  private final class JapaneseAnalyzerDiscardPunct extends Analyzer {
    // This is an approximation of an Analyzer that is actually built in Solr
    // using a config file.
    public JapaneseAnalyzerDiscardPunct() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
      final JapaneseTokenizer src = new JapaneseTokenizer(reader, null, true, JapaneseTokenizer.Mode.SEARCH);
      TokenStream tok = new JapaneseNumberFilter(src);
      tok = new JapaneseBaseFormFilter(src);
      tok = new CJKWidthFilter(src);
      tok = new JapaneseKatakanaStemFilter(src);
//      tok = new SynonymFilter(src, null, true);
      tok = new LowerCaseFilter(TEST_VERSION_CURRENT, src);
      return new TokenStreamComponents(src, tok);
    }
  }

  private final class JapaneseAnalyzerDontDiscardPunct extends Analyzer {
    // This is an approximation of an Analyzer that is actually built in Solr
    // using a config file.
    public JapaneseAnalyzerDontDiscardPunct() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
      final JapaneseTokenizer src = new JapaneseTokenizer(reader, null, false, JapaneseTokenizer.Mode.SEARCH);
      TokenStream tok = new JapaneseNumberFilter(src);
      tok = new JapaneseBaseFormFilter(src);
      tok = new CJKWidthFilter(src);
      tok = new JapaneseKatakanaStemFilter(src);
//      tok = new SynonymFilter(src, null, true);
      tok = new LowerCaseFilter(TEST_VERSION_CURRENT, src);
      return new TokenStreamComponents(src, tok);
    }
  }
  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  private static void checkQuery(WildcardNearQueryParser p, String query, String result) throws Exception {
    checkQuery(p, query, result, false);
  }

  private static void checkQuery(WildcardNearQueryParser p, String query, String result,
      boolean ignoreWC) throws Exception {
    Query q = p.parse(query, ignoreWC);
    String qs = q.toString();
    if (VERBOSE) {
      System.out.println("Check if equal '" + qs + "' == '" + result + "'");
    }
    assertEquals(qs, result);
  }

  private static void checkQueryEqual(WildcardNearQueryParser p, String query, Query expected_q,
      boolean ignoreWC) throws Exception {
    Query q = p.parse(query, ignoreWC);
    String qs = q.toString();
    if (VERBOSE) {
      System.out.println("Check if equal '" + qs + "' == '" + expected_q.toString() + "'");
    }
    assertTrue(q.equals(expected_q));
  }

  private static MultiTermQuery fixRewrite(MultiTermQuery q) {
    q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    return q;
  }

  public void testWildcardNearQueryParser() throws Exception {
    String field = "headline";
    boolean ignoreWC = false;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new BBFinancialStandardAnalyzer());

    checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
    checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);

    // Made up:
    FieldedQuery q = new OrderedNearQuery(0, new FieldedQuery[] {
        new TermQuery(new Term(field, "london")),
        new TermQuery(new Term(field, "city")),
        new TermQuery(new Term(field, "airport"))
    });
    checkQueryEqual(p, "London-City-Airport", q, ignoreWC);
    checkQueryEqual(p, "London City Airport", q, ignoreWC);

    q = new OrderedNearQuery(0, new FieldedQuery[] {
        new TermQuery(new Term(field, "london")),
        fixRewrite(new PrefixQuery(new Term(field, "cit"))),
        new TermQuery(new Term(field, "airport"))
    });
    checkQueryEqual(p, "London-Cit*-Airport", q, ignoreWC);

    q = new OrderedNearQuery(0, new FieldedQuery[] {
        new TermQuery(new Term(field, "london")),
        fixRewrite(new WildcardQuery(new Term(field, "*ty"))),
        new TermQuery(new Term(field, "airport"))
    });
    checkQueryEqual(p, "London-*ty-Airport", q, ignoreWC);

    q = new OrderedNearQuery(1, new FieldedQuery[] {
        new TermQuery(new Term(field, "london")),
        new TermQuery(new Term(field, "airport"))
    });
    checkQueryEqual(p, "London-*-Airport", q, ignoreWC);
    checkQueryEqual(p, "London-* Airport", q, ignoreWC);
    checkQueryEqual(p, "London *-Airport", q, ignoreWC);
    checkQueryEqual(p, "London * Airport", q, ignoreWC);

    q = new OrderedNearQuery(0, new FieldedQuery[] {
        new TermQuery(new Term(field, "london")),
        fixRewrite(new WildcardQuery(new Term(field, "?"))),
        new TermQuery(new Term(field, "airport"))
    });
    checkQueryEqual(p, "London ? Airport", q, ignoreWC);

    checkQuery(p, "London City * Airport", "OrderedNear/1:Filtered(+OrderedNear/0:Filtered(+headline:london +headline:city) +headline:airport)");

    q = new OrderedNearQuery(1, new FieldedQuery[] {
        fixRewrite(new PrefixQuery(new Term(field, "lon"))),
        fixRewrite(new WildcardQuery(new Term(field, "*port"))),
    });
    checkQueryEqual(p, "Lon*-* *port", q, ignoreWC);

    checkQuery(p, "Lon*-* Airport", "OrderedNear/1:Filtered(+headline:lon* +headline:airport)");
    checkQuery(p, "Lon*-City Airport", "OrderedNear/0:Filtered(+headline:lon* +headline:city +headline:airport)");

    checkQueryEqual(p, "Lon*", fixRewrite(new PrefixQuery(new Term(field, "lon"))), ignoreWC);

    checkQuery(p, "trick-or-treat", "OrderedNear/0:Filtered(+headline:trick +headline:or +headline:treat)");
    checkQuery(p, "trick-??-treat", "OrderedNear/0:Filtered(+headline:trick +headline:?? +headline:treat)");
    checkQuery(p, "trick-?*?-treat", "OrderedNear/0:Filtered(+headline:trick +headline:?*? +headline:treat)");
    checkQuery(p, "trick-**-treat", "OrderedNear/1:Filtered(+headline:trick +headline:treat)");
    checkQuery(p, "trick-??-?????", "OrderedNear/0:Filtered(+headline:trick +headline:?? +headline:?????)");
    checkQuery(p, "trick-*-?????", "OrderedNear/1:Filtered(+headline:trick +headline:?????)");
    checkQuery(p, "trick-??-* candy", "OrderedNear/1:Filtered(+OrderedNear/0:Filtered(+headline:trick +headline:??) +headline:candy)");
    checkQuery(p, "trick-*-treat", "OrderedNear/1:Filtered(+headline:trick +headline:treat)");
    checkQuery(p, "trick-*-*-treat", "OrderedNear/2:Filtered(+headline:trick +headline:treat)");
    checkQuery(p, "trick * * treat", "OrderedNear/2:Filtered(+headline:trick +headline:treat)");
    checkQuery(p, "i love trick-*-treat candy", "OrderedNear/1:Filtered(+OrderedNear/0:Filtered(+headline:i +headline:love +headline:trick) +OrderedNear/0:Filtered(+headline:treat +headline:candy))");

    checkQuery(p, "slow up?-side down?", "OrderedNear/0:Filtered(+headline:slow +headline:up? +headline:side +headline:down?)");
    checkQuery(p, "slow up??-side down??", "OrderedNear/0:Filtered(+headline:slow +headline:up?? +headline:side +headline:down??)");
    checkQuery(p, "slow-up??side down", "OrderedNear/0:Filtered(+headline:slow +headline:up??side +headline:down)");
    checkQuery(p, "slow-up-??side down", "OrderedNear/0:Filtered(+headline:slow +headline:up +headline:??side +headline:down)");

    checkQueryEqual(p, "up??side", fixRewrite(new WildcardQuery(new Term(field, "up??side"))), ignoreWC);

    // Based on real queries:
    checkQuery(p, "8-k*", "OrderedNear/0:Filtered(+headline:8 +headline:k*)");
    checkQuery(p, "re-domesticat*", "OrderedNear/0:Filtered(+headline:re +headline:domesticat*)");
    checkQuery(p, "re-domicile*", "OrderedNear/0:Filtered(+headline:re +headline:domicile*)");
    checkQuery(p, "ipc-fipe*", "OrderedNear/0:Filtered(+headline:ipc +headline:fipe*)");
    checkQuery(p, "leo-mesdag b.v.*", "OrderedNear/0:Filtered(+headline:leo +headline:mesdag +headline:b.v)");
    checkQuery(p, "spin-off*", "OrderedNear/0:Filtered(+headline:spin +headline:off*)");
    checkQuery(p, "skb-bank*", "OrderedNear/0:Filtered(+headline:skb +headline:bank*)");
    checkQuery(p, "gonzalez-paramo*", "OrderedNear/0:Filtered(+headline:gonzalez +headline:paramo*)");
    checkQuery(p, "jenaro cardona-fox*", "OrderedNear/0:Filtered(+headline:jenaro +headline:cardona +headline:fox*)");
    checkQuery(p, "t-note*", "OrderedNear/0:Filtered(+headline:t +headline:note*)");
    checkQuery(p, "non-bank*", "OrderedNear/0:Filtered(+headline:non +headline:bank*)");
    checkQuery(p, "conversion to open-end*", "OrderedNear/0:Filtered(+headline:conversion +headline:to +headline:open +headline:end*)");
    checkQuery(p, "estate uk-3*", "OrderedNear/0:Filtered(+headline:estate +headline:uk +headline:3*)");
    checkQuery(p, "sc-to-* sec filing", "OrderedNear/1:Filtered(+OrderedNear/0:Filtered(+headline:sc +headline:to) +OrderedNear/0:Filtered(+headline:sec +headline:filing))");
    checkQuery(p, "ВСМПО-АВИСМА*", "OrderedNear/0:Filtered(+headline:всмпо +headline:ависма*)");
    checkQuery(p, "jean-franc* dubos", "OrderedNear/0:Filtered(+headline:jean +headline:franc* +headline:dubos)");
    checkQuery(p, "vietnam-singapore industrial* park*", "OrderedNear/0:Filtered(+headline:vietnam +headline:singapore +headline:industrial* +headline:park*)");
    checkQuery(p, "prorated* or pro-rated", "OrderedNear/0:Filtered(+headline:prorated* +headline:or +headline:pro +headline:rated)");

    // Made up:
    checkQuery(p, "*or-me", "OrderedNear/0:Filtered(+headline:*or +headline:me)");
    checkQuery(p, "or*-me", "OrderedNear/0:Filtered(+headline:or* +headline:me)");

    // Based on real queries:
    checkQuery(p, "sc-to* sec filing", "OrderedNear/0:Filtered(+headline:sc +headline:to* +headline:sec +headline:filing)");
    checkQuery(p, "throw-in*", "OrderedNear/0:Filtered(+headline:throw +headline:in*)");
  }

  public void testWildcardNearQueryParserKeyword() throws Exception {
    String field = "agent_name";
    boolean ignoreWC = false;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new KeywordAnalyzer());

    checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
    checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);

    // Made up:
    checkQueryEqual(p, "London-City-Airport", new TermQuery(new Term(field, "London-City-Airport")), ignoreWC);
    checkQueryEqual(p, "London City Airport", new TermQuery(new Term(field, "London City Airport")), ignoreWC);
    checkQueryEqual(p, "London-Cit*-Airport", fixRewrite(new WildcardQuery(new Term(field, "London-Cit*-Airport"))), ignoreWC);
    checkQueryEqual(p, "London Cit* Airport", fixRewrite(new WildcardQuery(new Term(field, "London Cit* Airport"))), ignoreWC);

    checkQueryEqual(p, "Lon*", fixRewrite(new PrefixQuery(new Term(field, "Lon"))), ignoreWC);

    checkQueryEqual(p, "ngtr*", fixRewrite(new PrefixQuery(new Term(field, "ngtr"))), ignoreWC);
    checkQueryEqual(p, "bbotf*", fixRewrite(new PrefixQuery(new Term(field, "bbotf"))), ignoreWC);
}

  public void testWildcardNearQueryParserIgnoreWildcard() throws Exception {
    String field = "string";
    boolean ignoreWC = true;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new KeywordAnalyzer());

    checkQueryEqual(p, "*", new TermQuery(new Term(field, "*")), ignoreWC);
    checkQueryEqual(p, "?", new TermQuery(new Term(field, "?")), ignoreWC);

    // Made up:
    checkQueryEqual(p, "London-City-Airport", new TermQuery(new Term(field, "London-City-Airport")), ignoreWC);
    checkQueryEqual(p, "London City Airport", new TermQuery(new Term(field, "London City Airport")), ignoreWC);
    checkQueryEqual(p, "London-Cit*-Airport", new TermQuery(new Term(field, "London-Cit*-Airport")), ignoreWC);
    checkQueryEqual(p, "London Cit* Airport", new TermQuery(new Term(field, "London Cit* Airport")), ignoreWC);

    checkQueryEqual(p, "Lon*", new TermQuery(new Term(field, "Lon*")), ignoreWC);

    checkQueryEqual(p, "ngtr*", new TermQuery(new Term(field, "ngtr*")), ignoreWC);
    checkQueryEqual(p, "bbotf*", new TermQuery(new Term(field, "bbotf*")), ignoreWC);
  }

  public void testWildcardNearQueryParserJapaneseDiscardPunct() throws Exception {
    String field = "headline_ja";
    boolean ignoreWC = false;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new JapaneseAnalyzerDiscardPunct());

    FieldedQuery q2 = new OrderedNearQuery(0, new FieldedQuery[] {
        new TermQuery(new Term(field, "london")),
        fixRewrite(new PrefixQuery(new Term(field, "cit"))),
        new TermQuery(new Term(field, "airport"))
    });
    checkQueryEqual(p, "London Cit* Airport", q2, ignoreWC);
    checkQueryEqual(p, "London  Cit*  Airport  ", q2, ignoreWC);
    
    checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
    checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);

    // Made up:
    FieldedQuery q = new OrderedNearQuery(0, new FieldedQuery[] {
        new TermQuery(new Term(field, "london")),
        new TermQuery(new Term(field, "city")),
        new TermQuery(new Term(field, "airport"))
    });    
    checkQueryEqual(p, "London-City-Airport", q, ignoreWC);
    checkQueryEqual(p, "London City Airport", q, ignoreWC);
    
    q = new OrderedNearQuery(0, new FieldedQuery[] {
        new TermQuery(new Term(field, "london")),
        fixRewrite(new PrefixQuery(new Term(field, "cit"))),
        new TermQuery(new Term(field, "airport"))
    });    
    checkQueryEqual(p, "London-Cit*-Airport", q, ignoreWC);
    checkQueryEqual(p, "London Cit* Airport", q, ignoreWC);
    
    checkQueryEqual(p, "Lon*", fixRewrite(new PrefixQuery(new Term(field, "lon"))), ignoreWC);

    checkQuery(p,"1995 equity* 2003 equity* 2290 equity* 2306 equity* 2341 equity* " +
        "2427 equity* 2454 equity* 2812 equity* 2831 equity* 2927 equity* " +
        "3362 equity* 3375 equity* 3708 equity* 3740 equity* 4678 equity* " + 
        "5271 equity* 5280 equity* 5949 equity* 6286 equity* 6365 equity* " +
        "6476 equity* 6874 equity* 7219 equity* 7223 equity* 7265 equity* " +
        "7291 equity* 7292 equity* 7428 equity* 7473 equity* 7541 equity* " +
        "7718 equity* 7874 equity* 7880 equity* 8134 equity* 8208 equity* " +
        "8355 equity* 8358 equity* 8364 equity* 8567 equity* 9543 equity* " +
        "9817 equity* 9890 equity* 9939 equity* 9964 equity*",
        "OrderedNear/0:Filtered(" +
        "+headline_ja:1995 +headline_ja:equity* +headline_ja:2003 +headline_ja:equity* " +
        "+headline_ja:2290 +headline_ja:equity* +headline_ja:2306 +headline_ja:equity* " +
        "+headline_ja:2341 +headline_ja:equity* +headline_ja:2427 +headline_ja:equity* " +
        "+headline_ja:2454 +headline_ja:equity* +headline_ja:2812 +headline_ja:equity* " +
        "+headline_ja:2831 +headline_ja:equity* +headline_ja:2927 +headline_ja:equity* " +
        "+headline_ja:3362 +headline_ja:equity* +headline_ja:3375 +headline_ja:equity* " +
        "+headline_ja:3708 +headline_ja:equity* +headline_ja:3740 +headline_ja:equity* " +
        "+headline_ja:4678 +headline_ja:equity* +headline_ja:5271 +headline_ja:equity* " +
        "+headline_ja:5280 +headline_ja:equity* +headline_ja:5949 +headline_ja:equity* " +
        "+headline_ja:6286 +headline_ja:equity* +headline_ja:6365 +headline_ja:equity* " +
        "+headline_ja:6476 +headline_ja:equity* +headline_ja:6874 +headline_ja:equity* " +
        "+headline_ja:7219 +headline_ja:equity* +headline_ja:7223 +headline_ja:equity* " +
        "+headline_ja:7265 +headline_ja:equity* +headline_ja:7291 +headline_ja:equity* " +
        "+headline_ja:7292 +headline_ja:equity* +headline_ja:7428 +headline_ja:equity* " +
        "+headline_ja:7473 +headline_ja:equity* +headline_ja:7541 +headline_ja:equity* " +
        "+headline_ja:7718 +headline_ja:equity* +headline_ja:7874 +headline_ja:equity* " +
        "+headline_ja:7880 +headline_ja:equity* +headline_ja:8134 +headline_ja:equity* " +
        "+headline_ja:8208 +headline_ja:equity* +headline_ja:8355 +headline_ja:equity* " +
        "+headline_ja:8358 +headline_ja:equity* +headline_ja:8364 +headline_ja:equity* " +
        "+headline_ja:8567 +headline_ja:equity* +headline_ja:9543 +headline_ja:equity* " +
        "+headline_ja:9817 +headline_ja:equity* +headline_ja:9890 +headline_ja:equity* " +
        "+headline_ja:9939 +headline_ja:equity* +headline_ja:9964 +headline_ja:equity*" +
        ")");

    checkQueryEqual(p, "ngtr*", fixRewrite(new PrefixQuery(new Term(field, "ngtr"))), ignoreWC);
    checkQueryEqual(p, "bbotf*", fixRewrite(new PrefixQuery(new Term(field, "bbotf"))), ignoreWC);
  }

public void testWildcardNearQueryParserJapaneseDontDiscardPunct() throws Exception {
  String field = "body_ja";
  boolean ignoreWC = false;
  WildcardNearQueryParser p = new WildcardNearQueryParser(field, new JapaneseAnalyzerDontDiscardPunct());

  FieldedQuery q2 = new OrderedNearQuery(0, new FieldedQuery[] {
      new TermQuery(new Term(field, "london")),
      fixRewrite(new PrefixQuery(new Term(field, "cit"))),
      new TermQuery(new Term(field, "airport"))
  });
  checkQueryEqual(p, "London Cit* Airport", q2, ignoreWC);
  checkQueryEqual(p, "London  Cit*  Airport  ", q2, ignoreWC);
  
  checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
  checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);

  // Made up:
  FieldedQuery q = new OrderedNearQuery(0, new FieldedQuery[] {
      new TermQuery(new Term(field, "london")),
      new TermQuery(new Term(field, "-")),
      new TermQuery(new Term(field, "city")),
      new TermQuery(new Term(field, "-")),
      new TermQuery(new Term(field, "airport"))
  });    
  checkQueryEqual(p, "London-City-Airport", q, ignoreWC);
  q = new OrderedNearQuery(0, new FieldedQuery[] {
      new TermQuery(new Term(field, "london")),
      new TermQuery(new Term(field, "city")),
      new TermQuery(new Term(field, "airport"))
  });    
  checkQueryEqual(p, "London City Airport", q, ignoreWC);
  
  q = new OrderedNearQuery(0, new FieldedQuery[] {
      new TermQuery(new Term(field, "london")),
      fixRewrite(new PrefixQuery(new Term(field, "cit"))),
      new TermQuery(new Term(field, "airport"))
  });    
  checkQueryEqual(p, "London Cit* Airport", q, ignoreWC);

  q = new OrderedNearQuery(0, new FieldedQuery[] {
      new TermQuery(new Term(field, "london")),
      new TermQuery(new Term(field, "-")),
      fixRewrite(new WildcardQuery(new Term(field, "cit*-airport")))
  });    
  checkQueryEqual(p, "London-Cit*-Airport", q, ignoreWC);

  checkQueryEqual(p, "Lon*", fixRewrite(new PrefixQuery(new Term(field, "lon"))), ignoreWC);

  checkQuery(p,"1995 equity* 2003 equity* 2290 equity* 2306 equity* 2341 equity* " +
      "2427 equity* 2454 equity* 2812 equity* 2831 equity* 2927 equity* " +
      "3362 equity* 3375 equity* 3708 equity* 3740 equity* 4678 equity* " + 
      "5271 equity* 5280 equity* 5949 equity* 6286 equity* 6365 equity* " +
      "6476 equity* 6874 equity* 7219 equity* 7223 equity* 7265 equity* " +
      "7291 equity* 7292 equity* 7428 equity* 7473 equity* 7541 equity* " +
      "7718 equity* 7874 equity* 7880 equity* 8134 equity* 8208 equity* " +
      "8355 equity* 8358 equity* 8364 equity* 8567 equity* 9543 equity* " +
      "9817 equity* 9890 equity* 9939 equity* 9964 equity*",
      "OrderedNear/0:Filtered(" +
      "+body_ja:1995 +body_ja:equity* +body_ja:2003 +body_ja:equity* " +
      "+body_ja:2290 +body_ja:equity* +body_ja:2306 +body_ja:equity* " +
      "+body_ja:2341 +body_ja:equity* +body_ja:2427 +body_ja:equity* " +
      "+body_ja:2454 +body_ja:equity* +body_ja:2812 +body_ja:equity* " +
      "+body_ja:2831 +body_ja:equity* +body_ja:2927 +body_ja:equity* " +
      "+body_ja:3362 +body_ja:equity* +body_ja:3375 +body_ja:equity* " +
      "+body_ja:3708 +body_ja:equity* +body_ja:3740 +body_ja:equity* " +
      "+body_ja:4678 +body_ja:equity* +body_ja:5271 +body_ja:equity* " +
      "+body_ja:5280 +body_ja:equity* +body_ja:5949 +body_ja:equity* " +
      "+body_ja:6286 +body_ja:equity* +body_ja:6365 +body_ja:equity* " +
      "+body_ja:6476 +body_ja:equity* +body_ja:6874 +body_ja:equity* " +
      "+body_ja:7219 +body_ja:equity* +body_ja:7223 +body_ja:equity* " +
      "+body_ja:7265 +body_ja:equity* +body_ja:7291 +body_ja:equity* " +
      "+body_ja:7292 +body_ja:equity* +body_ja:7428 +body_ja:equity* " +
      "+body_ja:7473 +body_ja:equity* +body_ja:7541 +body_ja:equity* " +
      "+body_ja:7718 +body_ja:equity* +body_ja:7874 +body_ja:equity* " +
      "+body_ja:7880 +body_ja:equity* +body_ja:8134 +body_ja:equity* " +
      "+body_ja:8208 +body_ja:equity* +body_ja:8355 +body_ja:equity* " +
      "+body_ja:8358 +body_ja:equity* +body_ja:8364 +body_ja:equity* " +
      "+body_ja:8567 +body_ja:equity* +body_ja:9543 +body_ja:equity* " +
      "+body_ja:9817 +body_ja:equity* +body_ja:9890 +body_ja:equity* " +
      "+body_ja:9939 +body_ja:equity* +body_ja:9964 +body_ja:equity*" +
      ")");

  checkQueryEqual(p, "ngtr*", fixRewrite(new PrefixQuery(new Term(field, "ngtr"))), ignoreWC);
  checkQueryEqual(p, "bbotf*", fixRewrite(new PrefixQuery(new Term(field, "bbotf"))), ignoreWC);
}
}
