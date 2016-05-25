package org.apache.solr.ltr.log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.ltr.feature.LTRScoringAlgorithm;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.util.NamedParams;

/**
 * a stubbed reranking model that will be used only for computing the features.
 **/
public class LoggingModel extends LTRScoringAlgorithm {

  final static String LOGGING_MODEL_NAME = "logging-model";

  public LoggingModel(String featureStoreName, Collection<Feature> allFeatures){
    this(LOGGING_MODEL_NAME, Collections.emptyList(), featureStoreName, allFeatures, NamedParams.EMPTY);
  }


  protected LoggingModel(String name, List<Feature> features, String featureStoreName,
      Collection<Feature> allFeatures, NamedParams params) {
    super(name, features, featureStoreName, allFeatures, params);
  }

  @Override
  public float score(float[] modelFeatureValuesNormalized) {
    return 0;
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc, float finalScore, List<Explanation> featureExplanations) {
    return Explanation.match(finalScore, toString()
        + " logging model, used only for logging the features");
  }

}
