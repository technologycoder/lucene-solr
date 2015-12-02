package ltr.feature.impl;

import java.io.IOException;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This feature will return the score of the original query performed on solr. 
 */
public class OriginalScoreFeature extends Feature {

  private static final Logger logger = LoggerFactory.getLogger(OriginalScoreFeature.class);

  @Override
  public OriginalScoreWeight createWeight(IndexSearcher searcher) throws IOException {
    return new OriginalScoreWeight(searcher, name, params, norm, id);

  }

  public class OriginalScoreWeight extends FeatureWeight {

    Weight w = null;

    public OriginalScoreWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);

    }
    
    public void setOriginalQuery(Query originalQuery) throws IOException  {
      // I can't set w before in the constructor because I would need to have it
      // in the query for doing that. But the query/feature is shared among
      // different threads
      // so I can't set the original query there.
      if (originalQuery == null){
        logger.warn("cannot find original query ");
        originalQuery = new MatchAllDocsQuery();
      }
      w = searcher.createNormalizedWeight(originalQuery);
    }

    public void process() throws IOException {
      // I can't set w before in the constructor because I would need to have it
      // in the query for doing that. But the query/feature is shared among
      // different threads
      // so I can't set the original query there.
      if (originalQuery == null) {
        logger.warn("cannot find original query ");
        originalQuery = new MatchAllDocsQuery();
      }
      // different threads so I can't set the original query there.
      w = searcher.createNormalizedWeight(this.originalQuery);
    };

    @Override
    public Query getQuery() {
      return OriginalScoreFeature.this;
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      // Explanation e = w.explain(context, doc);
      Scorer s = w.scorer(context, PostingFeatures.DOCS_AND_FREQS, context.reader().getLiveDocs());
      s.advance(doc);
      float score = s.score();
      return new Explanation(score, "original score query: " + originalQuery);
    }

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {

      Scorer originalScorer = w.scorer(context, PostingFeatures.DOCS_AND_FREQS, acceptDocs);
      return new OriginalScoreScorer(this, originalScorer);
    }

    public class OriginalScoreScorer extends FeatureScorer {
      Scorer originalScorer;

      public OriginalScoreScorer(FeatureWeight weight, Scorer originalScorer) {
        super(weight);
        this.originalScorer = originalScorer;
      }

      @Override
      public int advance(int target) throws IOException {
        docID = originalScorer.advance(target);
        return docID;
      }

      @Override
      public float score() throws IOException {
        if (originalQuery instanceof MatchAllDocsQuery){
          return getDefaultScore();
        }
        float score = originalScorer.score();
        return score;
      }

      @Override
      public String toString() {
        return "OriginalScoreFeature [query:" + originalQuery.toString() + "]";
      }
    }

  }

}
