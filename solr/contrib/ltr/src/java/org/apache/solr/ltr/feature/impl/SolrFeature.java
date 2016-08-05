package org.apache.solr.ltr.feature.impl;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrIndexSearcher.ProcessedFilter;
import org.apache.solr.search.SyntaxError;

public class SolrFeature extends Feature {

  private String df;
  private String q;
  private List<String> fq;

  public String getDf() {
    return df;
  }

  public void setDf(String df) {
    this.df = df;
  }

  public String getQ() {
    return q;
  }

  public void setQ(String q) {
    this.q = q;
  }

  public List<String> getFq() {
    return fq;
  }

  public void setFq(List<String> fq) {
    this.fq = fq;
  }

  @Override
  protected LinkedHashMap<String,Object> paramsToMap() {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<>(3, 1.0f);
    if (df != null) {
      params.put("df", df);
    }
    if (q != null) {
      params.put("q", q);
    }
    if (fq != null) {
      params.put("fq", fq);
    }
    return params;
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String,String> efi)
      throws IOException {
    return new SolrFeatureWeight(searcher, request, originalQuery, efi);
  }

  public class SolrFeatureWeight extends FeatureWeight {
    Weight solrQueryWeight;
    Query query;
    List<Query> queryAndFilters;

    public SolrFeatureWeight(IndexSearcher searcher, 
        SolrQueryRequest request, Query originalQuery, Map<String,String> efi) throws IOException {
      super(SolrFeature.this, searcher, request, originalQuery, efi);
      try {
        String solrQuery = q;
        final List<String> fqs = fq;

        if (((solrQuery == null) || solrQuery.isEmpty())
            && ((fqs == null) || fqs.isEmpty())) {
          throw new IOException("ERROR: FQ or Q have not been provided");
        }

        if ((solrQuery == null) || solrQuery.isEmpty()) {
          solrQuery = "*:*";
        }

        solrQuery = macroExpander.expand(solrQuery);
        if (solrQuery == null) {
          throw new FeatureException(this.getClass().getSimpleName()+" requires efi parameter that was not passed in request.");
        }

        final SolrQueryRequest req = makeRequest(request.getCore(), solrQuery,
            fqs, df);
        if (req == null) {
          throw new IOException("ERROR: No parameters provided");
        }

        // Build the filter queries
        queryAndFilters = new ArrayList<Query>(); // If there are no fqs we
                                                  // just want an empty
                                                  // list
        if (fqs != null) {
          for (String fq : fqs) {
            if ((fq != null) && (fq.trim().length() != 0)) {
              fq = macroExpander.expand(fq);
              final QParser fqp = QParser.getParser(fq, null, req);
              final Query filterQuery = fqp.getQuery();
              if (filterQuery != null) {
                queryAndFilters.add(filterQuery);
              }
            }
          }
        }

        final QParser parser = QParser.getParser(solrQuery, null, req);
        query = parser.parse();

        // Query can be null if there was no input to parse, for instance if you
        // make a phrase query with "to be", and the analyzer removes all the
        // words
        // leaving nothing for the phrase query to parse.
        if (query != null) {
          queryAndFilters.add(query);
          solrQueryWeight = searcher.createNormalizedWeight(query, true);
        }
      } catch (final SyntaxError e) {
        throw new FeatureException("Failed to parse feature query.", e);
      }
    }

    private LocalSolrQueryRequest makeRequest(SolrCore core, String solrQuery,
        List<String> fqs, String df) {
      // Map.Entry<String, String> [] entries = new NamedListEntry[q.length /
      // 2];
      final NamedList<String> returnList = new NamedList<String>();
      if ((solrQuery != null) && !solrQuery.isEmpty()) {
        returnList.add(CommonParams.Q, solrQuery);
      }
      if (fqs != null) {
        for (final String fq : fqs) {
          returnList.add(CommonParams.FQ, fq);
          // entries[i/2] = new NamedListEntry<>(q[i], q[i+1]);
        }
      }
      if ((df != null) && !df.isEmpty()) {
        returnList.add(CommonParams.DF, df);
      }
      if (returnList.size() > 0) {
        return new LocalSolrQueryRequest(core, returnList);
      } else {
        return null;
      }
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      Scorer solrScorer = null;
      if (solrQueryWeight != null) {
        solrScorer = solrQueryWeight.scorer(context);
      }

      final DocIdSetIterator idItr = getDocIdSetIteratorFromQueries(
          queryAndFilters, context);
      if (idItr != null) {
        return solrScorer == null ? new SolrFeatureFilterOnlyScorer(this, idItr)
            : new SolrFeatureScorer(this, solrScorer, idItr);
      } else {
        return null;
      }
    }

    /**
     * Given a list of Solr filters/queries, return a doc iterator that
     * traverses over the documents that matched all the criteria of the
     * queries.
     *
     * @param queries
     *          Filtering criteria to match documents against
     * @param context
     *          Index reader
     * @return DocIdSetIterator to traverse documents that matched all filter
     *         criteria
     */
    public DocIdSetIterator getDocIdSetIteratorFromQueries(List<Query> queries,
        LeafReaderContext context) throws IOException {
      // FIXME: Only SolrIndexSearcher has getProcessedFilter(), but all weights
      // are given an IndexSearcher instead.
      // Ideally there should be some guarantee that we have a SolrIndexSearcher
      // so we don't have to cast.
      final ProcessedFilter pf = ((SolrIndexSearcher) searcher)
          .getProcessedFilter(null, queries);
      final Bits liveDocs = context.reader().getLiveDocs();

      DocIdSetIterator idIter = null;
      if (pf.filter != null) {
        final DocIdSet idSet = pf.filter.getDocIdSet(context, liveDocs);
        if (idSet != null) {
          idIter = idSet.iterator();
        }
      }

      return idIter;
    }

    public class SolrFeatureScorer extends FeatureScorer {
      final private Scorer solrScorer;
      final private DocIdSetIterator itr;

      public SolrFeatureScorer(FeatureWeight weight, Scorer solrScorer,
          DocIdSetIterator filterIterator) {
        super(weight);
        this.solrScorer = solrScorer;
        itr = new SolrFeatureScorerIterator(filterIterator,
            solrScorer.iterator());
      }

      @Override
      public float score() throws IOException {
        return solrScorer.score();
      }

      @Override
      public DocIdSetIterator iterator() {
        return itr;
      }

      @Override
      public int docID() {
        return itr.docID();
      }

      private class SolrFeatureScorerIterator extends DocIdSetIterator {

        final private DocIdSetIterator filterIterator;
        final private DocIdSetIterator scorerFilter;
        int docID;

        SolrFeatureScorerIterator(DocIdSetIterator filterIterator,
            DocIdSetIterator scorerFilter) {
          this.filterIterator = filterIterator;
          this.scorerFilter = scorerFilter;
        }

        @Override
        public int docID() {
          return filterIterator.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          docID = filterIterator.nextDoc();
          scorerFilter.advance(docID);
          return docID;
        }

        @Override
        public int advance(int target) throws IOException {
          // We use iterator to catch the scorer up since
          // that checks if the target id is in the query + all the filters
          docID = filterIterator.advance(target);
          scorerFilter.advance(docID);
          return docID;
        }

        @Override
        public long cost() {
          return 0; // FIXME: Make this work?
        }

      }
    }

    public class SolrFeatureFilterOnlyScorer extends FeatureScorer {
      final private DocIdSetIterator itr;

      public SolrFeatureFilterOnlyScorer(FeatureWeight weight,
          DocIdSetIterator iterator) {
        super(weight);
        itr = iterator;
      }

      @Override
      public float score() throws IOException {
        return 1f;
      }

      @Override
      public DocIdSetIterator iterator() {
        return itr;
      }

      @Override
      public int docID() {
        return itr.docID();
      }

    }

  }

}
