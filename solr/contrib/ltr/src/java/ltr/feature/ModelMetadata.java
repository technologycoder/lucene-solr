package ltr.feature;

import java.util.Collection;
import java.util.List;

import ltr.ranking.Feature;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;

/**
 * Contains all the data needed for loading a model.
 */
// FIXME: Rename to something like RankingAlgorithm or ScoringAlgorithm
public abstract class ModelMetadata {

  private final String name;
  private final String type;
  private final String featureStoreName;
  private final List<Feature> features;
  private final Collection<Feature> allFeatures;
  private final NamedParams params;

  /**
   * Create a model that will be used to rerank the documents
   * @param name the name of this model
   * @param type the java class name of this model
   * @param features the feature object used by this
   * @param featureStoreName the feature store used by this model
   * @param allFeatures all the feature contained in the feature store
   * @param params optional parameters used by the model
   */
  public ModelMetadata(final String name, final String type, final List<Feature> features, final String featureStoreName, final Collection<Feature> allFeatures, final NamedParams params) {
    this.name = name;
    this.type = type;
    this.features = features;
    this.featureStoreName = featureStoreName;
    this.allFeatures = allFeatures;
    this.params = params;
  }

  /**
   * @return the name
   */
  public String getName() {
    return this.name;
  }

  /**
   * @return the type
   */
  public String getType() {
    return this.type;
  }

  /**
   * @return the features
   */
  public List<Feature> getFeatures() {
    return this.features;
  }

  /**
   * @return the params
   */
  public NamedParams getParams() {
    return this.params;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((this.features == null) ? 0 : this.features.hashCode());
    result = (prime * result) + ((this.name == null) ? 0 : this.name.hashCode());
    result = (prime * result) + ((this.params == null) ? 0 : this.params.hashCode());
    result = (prime * result) + ((this.type == null) ? 0 : this.type.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    final ModelMetadata other = (ModelMetadata) obj;
    if (this.features == null) {
      if (other.features != null) {
        return false;
      }
    } else if (!this.features.equals(other.features)) {
      return false;
    }
    if (this.name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!this.name.equals(other.name)) {
      return false;
    }
    if (this.params == null) {
      if (other.params != null) {
        return false;
      }
    } else if (!this.params.equals(other.params)) {
      return false;
    }
    if (this.type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!this.type.equals(other.type)) {
      return false;
    }
    return true;
  }

  public boolean hasParams() {
    return !((this.params == null) || this.params.isEmpty());
  }


  public Collection<Feature> getAllFeatures() {
    return this.allFeatures;
  }

  /**
   * @return the featureStore
   */
  public String getFeatureStoreName() {
    return this.featureStoreName;
  }

  /**
   * Given a list of normalized values for all features a scoring algorithm cares about, calculate
   * and return a score.
   *
   * @param modelFeatureValuesNormalized
   *          List of normalized feature values. Each feature is identified by its id, which is the
   *          index in the array
   * @return The final score for a document
   */
  public abstract float score(float[] modelFeatureValuesNormalized);

  /**
   * Similar to the score() function, except it returns an explanation of how the features were
   * used to calculate the score.
   *
   * @param context Context the document is in
   * @param doc Document to explain
   * @param finalScore Original score
   * @param featureExplanations Explanations for each feature calculation
   * @return Explanation for the scoring of a doument
   */
  public abstract Explanation explain(AtomicReaderContext context, int doc, float finalScore, List<Explanation> featureExplanations);

}
