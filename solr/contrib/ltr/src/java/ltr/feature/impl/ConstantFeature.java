package ltr.feature.impl;

import java.io.IOException;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.CommonLtrParams;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;

/**
 * This feature will produce a constant value for each document. 
 * The value can be passed by setting the param "value" into the configuration.
 */
public class ConstantFeature extends Feature {

  float value = -1f;

  public ConstantFeature() {

  }
  
  

  /**
   * @return the value
   */
  public float getValue() {
    return value;
  }



  /**
   * @param value the value to set
   */
  public void setValue(float value) {
    this.value = value;
  }



  @Override
  public FeatureWeight createWeight(IndexSearcher searcher) throws IOException {
    value = params.getFloat(CommonLtrParams.VALUE, value);
    return new ConstantFeatureWeight(searcher, name, params, norm, id);
  }

  public class ConstantFeatureWeight extends FeatureWeight {

    public ConstantFeatureWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return ConstantFeature.this;
    }

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      return new ConstantFeatureScorer(this, value, "ConstantFeature");
    }

  }

}
