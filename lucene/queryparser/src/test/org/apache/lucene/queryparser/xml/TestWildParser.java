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
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.standard.BBFinancialStandardTokenizer;
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
}
