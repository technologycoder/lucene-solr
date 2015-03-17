package org.apache.lucene.queryparser.xml.builders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;


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

/* Builder for keyword phrases specially for wildcarded multi term queries.
 * Text phrases can be thrown into this builder to get tokenized to form OrderedNearQuery of sub queries of individual tokens. 
 * Currently this can result in WildcardQuery,PrefixQuery and TermQuery as its sub queries. */

public class KeywordNearQueryParser {
  
  protected Analyzer analyzer;
  protected String field;
  private static final char WILDCARD_STRING = '*';
  private static final char WILDCARD_CHAR = '?';
  private static final char WILDCARD_ESCAPE = '\\';
  
  private enum WildcardState {
    TRAILING_WILDCARD,
    NON_TRAILING_WILDCARD,
    NON_WILDCARD
  }
  
  public KeywordNearQueryParser(String field, Analyzer analyzer) {
    this.field = field;
    this.analyzer = analyzer;
  }
  
  public Query parse(String text) throws ParserException {
    // to hold the sub queries until we generate the NearQuery
    ArrayList<FieldedQuery> queries = new ArrayList<>();
    ArrayList<Integer> positions = new ArrayList<>();
    int position = -1;
    // if there is any wildcard characters, do white space tokenization to get
    // the individual tokens and
    // then apply configured analysers on non wildcarded tokens
    
    // TODO: this would have been much nicer if we could chain the whitespacetokenizer 
    // to the main analyser chain just before the tokenizer of the main chain and breaking out of the analysis on finding wildcards.
    if (checkWildcard(text) != WildcardState.NON_WILDCARD) {
      WhitespaceAnalyzer wa = new WhitespaceAnalyzer(
          org.apache.lucene.util.Version.LUCENE_CURRENT);
      TokenStream source = null;
      try {
        source = wa.tokenStream("ws_delimiter", text);//field name here is anyway a dummy name
        source.reset();
        
        TermToBytesRefAttribute termAtt = null;
        BytesRef bytes = null;
        if (source.hasAttribute(TermToBytesRefAttribute.class)) {
          termAtt = source.getAttribute(TermToBytesRefAttribute.class);
          bytes = termAtt.getBytesRef();
        } else throw new ParserException("Cannot build keyword query, "
            + "token stream has no TermToBytesRefAttribute. field:" + field
            + ", text:" + text);
        
        while (source.incrementToken()) {
          termAtt.fillBytesRef();
          String token = bytes.utf8ToString();
          WildcardState wcs = checkWildcard(token);
          switch (wcs) {
            case TRAILING_WILDCARD:
              token = token.toLowerCase(Locale.getDefault());
              PrefixQuery pq = new PrefixQuery(new Term(field, token.substring(0,
                  token.length() - 1)));
              ((MultiTermQuery)pq).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
              queries.add(pq);
              positions.add(++position);
              break;
            case NON_TRAILING_WILDCARD:
              token = token.toLowerCase(Locale.getDefault());
              WildcardQuery wq = new WildcardQuery(new Term(field, token));
              ((MultiTermQuery)wq).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
              queries.add(wq);
              positions.add(++position);
              break;
            case NON_WILDCARD:
              position = analysePhrases(token, queries, positions, position);
          }
          
        }
        source.end();
      } catch (IOException ioe) {
        ParserException p = new ParserException("Cannot build keyword query on field:"
            + field + ", text:" + text);
        p.initCause(ioe);
        throw p;
      } finally {
        IOUtils.closeWhileHandlingException(source);
      }
    } else {
      analysePhrases(text, queries, positions, position);
    }
    
    if(queries.isEmpty())
      return new MatchAllDocsQuery();
    else
        return makeSubQuery(queries,positions, 0, queries.size()-1);
  }
  
  //
  private FieldedQuery makeSubQuery(ArrayList<FieldedQuery> queries, ArrayList<Integer> positions, int startIndex, int stopIndex){
    if(startIndex == stopIndex)
      return queries.get(startIndex);
    
    int index = startIndex;
    final int groupable_distance = 1; //if the tokens are far apart than this distance then it is a candidate for near query.
    while (index < stopIndex) {
      int distance = positions.get(index+1)- positions.get(index);
      if(distance == groupable_distance){
        index++;
        continue;
      }
      else {
        FieldedQuery[] subQueries = new FieldedQuery[2];
        subQueries[0] = makeSubQuery(queries, positions, startIndex, index);
        subQueries[1] = makeSubQuery(queries, positions, index+1, stopIndex);
        return new OrderedNearQuery(distance-1, subQueries);
      }
    }
    
    final int numberOfElementstoCopy= stopIndex-startIndex+1;
    FieldedQuery[] subQueries = new FieldedQuery[numberOfElementstoCopy];
    for(int i=0; i < numberOfElementstoCopy; i++)
    {
      subQueries[i] = queries.get(startIndex + i);
    }
    return new OrderedNearQuery(groupable_distance-1/*slop*/, subQueries);
  }
  
  //analyses the text and fills up the individual term queries and positions in the respective array lists 
  //and returns the position increment after the last query token. This increment can be used to chain up the rest of the phrases to this query list.
  private int analysePhrases(String text,
      ArrayList<FieldedQuery> queries, ArrayList<Integer> positions, int currentPosition) throws ParserException {
    TokenStream source = null;
    try {
      source = analyzer.tokenStream(field, text);
      source.reset();
      
      TermToBytesRefAttribute termAtt = null;
      BytesRef bytes = null;
      if (source.hasAttribute(TermToBytesRefAttribute.class)) {
        termAtt = source.getAttribute(TermToBytesRefAttribute.class);
        bytes = termAtt.getBytesRef();
      } else throw new ParserException(
          "Cannot build query token stream has no TermToBytesRefAttribute. field:"
              + field + ", text:" + text);
      
      PositionIncrementAttribute posIncrAtt = null;
      if (source.hasAttribute(PositionIncrementAttribute.class)) {
        posIncrAtt = source.getAttribute(PositionIncrementAttribute.class);
      }
      
      while (source.incrementToken()) {
        int positionIncrement = (posIncrAtt != null) ? posIncrAtt
            .getPositionIncrement() : 1;
        currentPosition += positionIncrement;
        termAtt.fillBytesRef();
        queries.add(new TermQuery(new Term(field, BytesRef.deepCopyOf(bytes))));
        positions.add(currentPosition);
      }
      source.end();
      if (posIncrAtt != null)
        currentPosition += posIncrAtt.getPositionIncrement();
    } catch (IOException ioe) {
      ParserException p = new ParserException("Cannot build query on field:"
          + field + ", text:" + text);
      p.initCause(ioe);
      throw p;
    } finally {
      IOUtils.closeWhileHandlingException(source);
    }
    return currentPosition;
  }
  
  private static WildcardState checkWildcard(String text) {
    for (int i = 0; i < text.length();) {
      final int c = text.codePointAt(i);
      int length = Character.charCount(c);
      switch (c) {
        case WILDCARD_STRING:
        case WILDCARD_CHAR:
          if (i + length == text.length()) 
            return WildcardState.TRAILING_WILDCARD;
          else
            return WildcardState.NON_TRAILING_WILDCARD;
        case WILDCARD_ESCAPE:
          // skip over the escaped code point if it exists
          if (i + length < text.length()) {
            final int nextChar = text.codePointAt(i + length);
            length += Character.charCount(nextChar);
          }
          break;
      }
      i += length;
    }
    return WildcardState.NON_WILDCARD;
  }
  
}
