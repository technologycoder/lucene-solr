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
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.ranking.FeatureScorer;
import org.apache.solr.ltr.ranking.FeatureWeight;
import org.apache.solr.ltr.util.CommonLTRParams;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.request.SolrQueryRequest;

public class OriginalScoreFeature extends Feature {

  @Override
  public OriginalScoreWeight createWeight(IndexSearcher searcher,
      boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String,String> efi) throws IOException {
    return new OriginalScoreWeight(searcher, name, params, norm, id, request, originalQuery, efi);

  }

  public class OriginalScoreWeight extends FeatureWeight {

    final Weight w;

    public OriginalScoreWeight(IndexSearcher searcher, String name,
        NamedParams params, Normalizer norm, int id, SolrQueryRequest request, Query originalQuery, Map<String,String> efi) throws IOException {
      super(OriginalScoreFeature.this, searcher, name, params, norm, id, request, originalQuery, efi);
      w = searcher.createNormalizedWeight(originalQuery, true);
    };

    @Override
    public Explanation explain(LeafReaderContext context, int doc)
        throws IOException {
      // Explanation e = w.explain(context, doc);
      final Scorer s = w.scorer(context);
      s.iterator().advance(doc);
      final float score = s.score();
      return Explanation.match(score, "original score query: " + originalQuery);
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {

      final Scorer originalScorer = w.scorer(context);
      return new OriginalScoreScorer(this, originalScorer);
    }

    public class OriginalScoreScorer extends FeatureScorer {
      Scorer originalScorer;

      public OriginalScoreScorer(FeatureWeight weight, Scorer originalScorer) {
        super(weight);
        this.originalScorer = originalScorer;
      }

      @Override
      public float score() throws IOException {
        // This is done to improve the speed of feature extraction. Since this
        // was already scored in step 1
        // we shouldn't need to calc original score again.
        return hasDocParam(CommonLTRParams.ORIGINAL_DOC_SCORE) ? (Float) getDocParam(CommonLTRParams.ORIGINAL_DOC_SCORE)
            : originalScorer.score();
      }

      @Override
      public String toString() {
        return "OriginalScoreFeature [query:" + originalQuery.toString() + "]";
      }

      @Override
      public int docID() {
        return originalScorer.docID();
      }

      @Override
      public DocIdSetIterator iterator() {
        return originalScorer.iterator();
      }
    }

  }

}
