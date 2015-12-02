package ltr.feature.impl;

import java.io.IOException;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
/**
 * Allows to use a function query as a feature. In case of error the feature will 
 * always produce the value Float.MAXVALUE
 * FIXME put example.
 */
public class FunctionQueryFeature extends Feature {

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher) throws IOException {
    return new FunctionQueryWeight(searcher, name, params, norm, id);
  }

  public class FunctionQueryWeight extends FeatureWeight {
   
    public FunctionQueryWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
    }


    @Override
    public Query getQuery() {
      return FunctionQueryFeature.this;
    }

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      String fq = (String) getParams().get(CommonParams.FQ);
      Query query = null;
      FunctionQParser parser = new FunctionQParser(fq, null, null, request);
      try {
        query = parser.parse();
      } catch (SyntaxError e) {
        // FIXME log? throw exception?
        // set a constant feature value to Float.Max Value in case of error
        ConstantFeature feature = new ConstantFeature();
        feature.setValue(Float.MAX_VALUE);
        query = feature;
      }
      Weight w = null;
      query.setBoost(1);
      w = query.createWeight(searcher);
      w.getValueForNormalization();

      Scorer functionScorer = w.scorer(context, Weight.PostingFeatures.DOCS_AND_FREQS, acceptDocs);

      return new FunctionQueryFeatureScorer(this, functionScorer);
    }

    public class FunctionQueryFeatureScorer extends FeatureScorer {
      String fq = "";
      Scorer functionScorer;

      public FunctionQueryFeatureScorer(FeatureWeight weight, Scorer functionScorer) {
        super(weight);
        this.fq = (String) params.get(CommonParams.FQ);
        this.functionScorer = functionScorer;
      }

      @Override
      public int advance(int target) throws IOException {
        docID = functionScorer.advance(target);
        return docID;
      }

      @Override
      public float score() throws IOException {
        return functionScorer.score();
      }

      @Override
      public String toString() {
        return "FunctionQueryFeature [function:" + fq + "]";
      }
    }

  }

}
