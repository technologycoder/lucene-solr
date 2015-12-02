package ltr.model.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import ltr.feature.FeatureStore;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.FeatureException;
import ltr.util.ModelException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a stubbed reranking model that will be used only for 
 * computing the features.
 **/
public class LoggingModel extends Model {

  private static final Logger logger = LoggerFactory.getLogger(LoggingModel.class);
  FeatureStore store = null;

  public LoggingModel(FeatureStore store) throws FeatureException {
    this.store = store;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    // FIXME: Each model should own its list of global features for logging, in
    // case different models
    // want to log different sets of features.
    Collection<Feature> features = store.getFeatures();
    List<Feature> modelFeatures = Collections.emptyList();
    return new LoggingModelWeight(searcher, getWeights(modelFeatures, searcher), getWeights(features, searcher));
  }

  public class LoggingModelWeight extends ModelWeight {

    public LoggingModelWeight(IndexSearcher searcher, FeatureWeight[] modelFeatures, FeatureWeight[] allFeatures) {
      super(searcher, modelFeatures, allFeatures);
    }

    @Override
    protected ModelScorer makeModelScorer(ModelWeight weight, FeatureScorer[] featureScorers) {
      return new LoggingModelScorer(weight, featureScorers);
    }

    public class LoggingModelScorer extends ModelScorer {

      protected LoggingModelScorer(Weight weight, FeatureScorer[] featureScorers) {
        super(weight, featureScorers);
      }

      @Override
      public float score() throws IOException { // FIXME: Look at making default
                                                // logic later.
        {
          for (int i = 0; i < allFeatureScorers.length; i++) {
            FeatureScorer scorer = allFeatureScorers[i];
            if (scorer.docID() != doc) {
              allFeatureValues[i] = scorer.getDefaultScore();
            } else {
              try {
                allFeatureValues[i] = scorer.score();
              } catch (Exception e) {
                allFeatureValues[i] = scorer.getDefaultScore();
                logger.error("error computing feature {}, {}", allFeatureNames[i], e.getMessage());
              }
            }
          }

          return 1;
        }
      }

      @Override
      public IntervalIterator intervals(boolean collectIntervals) throws IOException {
        throw new UnsupportedOperationException();
      }

    }

  }

  @Override
  public Model replicate() throws ModelException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected ModelWeight makeModelWeight(IndexSearcher searcher, FeatureWeight[] modelFeatures, FeatureWeight[] allFeatures) {
    throw new UnsupportedOperationException();
  }

}
