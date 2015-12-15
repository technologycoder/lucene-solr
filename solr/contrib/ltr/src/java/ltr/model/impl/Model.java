package ltr.model.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import ltr.feature.ModelMetadata;
import ltr.feature.norm.Normalizer;
import ltr.feature.norm.impl.IdentityNormalizer;
import ltr.log.FeatureLogger;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.ModelException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A learned model, given a document context and a list of features predict a
 * relevance score.
 *
 */
public abstract class Model extends Query {
  // A model will produce a score for each document given a query so 
  // it is considered a scoring function, and extends the lucene Query object.

  // contains a description of the model
  protected ModelMetadata meta;

  // feature logger to output the features.
  private FeatureLogger<?> fl = FeatureLogger.getFeatureLogger(FeatureLogger.Format.CSV, 50);  

  // Map of external parameters, such as query intent, that can be used by
  // features
  protected Map<String,String> efi;

  // Original solr query used to fetch matching documents
  protected Query originalQuery;

  // Original solr request
  protected SolrQueryRequest request;

  public void init(ModelMetadata meta) throws ModelException {
    this.meta = meta;
  }

  // return an instance of this model
  public abstract Model replicate() throws ModelException;

  public ModelMetadata getMetadata() {
    return meta;
  }

  //public abstract float score(float[] modelFeatureValuesNormalized);

  //public abstract Query getModelQuery();

  public void setFeatureLogger(FeatureLogger<?> fl) {
    this.fl = fl;
  }

  public FeatureLogger<?> getFeatureLogger() {
    return this.fl;
  }
  
  public Collection<Feature> getAllFeatures(){
    return meta.getAllFeatures();
  }

  public void setOriginalQuery(Query mainQuery) {
    this.originalQuery = mainQuery;
  }

  /**
   * @param externalFeatureInfo
   */
  public void setExternalFeatureInfo(Map<String,String> externalFeatureInfo) {
    this.efi = externalFeatureInfo;
  }

  public void setRequest(SolrQueryRequest request) {
    this.request = request;
  }

  public Explanation explain(AtomicReaderContext context, int doc, float finalScore, List<Explanation> featureExplanations) {
    Explanation e = new Explanation(finalScore, meta.getName() + " [ " + meta.getType() + " ] model applied to features");
    for (Explanation featureExplain : featureExplanations) {
      e.addDetail(featureExplain);
    }
    return e;

  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((meta == null) ? 0 : meta.hashCode());
    result = prime * result + ((originalQuery == null) ? 0 : originalQuery.hashCode());
    result = prime * result + ((efi == null) ? 0 : originalQuery.hashCode());
    result = prime * result + this.toString().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj))
      return false;
    Model other = (Model) obj;
    if (meta == null) {
      if (other.meta != null)
        return false;
    } else if (!meta.equals(other.meta))
      return false;
    if (originalQuery == null) {
      if (other.originalQuery != null)
        return false;
    } else if (!originalQuery.equals(other.originalQuery))
      return false;
    return true;
  }

  public SolrQueryRequest getRequest() {
    return request;
  }

  /**
   * @return
   */
  public List<Feature> getFeatures() {
    return meta.getFeatures();
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    Collection<Feature> features = this.getAllFeatures();
    List<Feature> modelFeatures = this.getFeatures();
    //return new ModelWeight(searcher, getWeights(modelFeatures, searcher), getWeights(features, searcher), this);
    return makeModelWeight(searcher, getWeights(modelFeatures, searcher), getWeights(features, searcher));
  }
  
  protected abstract ModelWeight makeModelWeight(IndexSearcher searcher, FeatureWeight[] modelFeatures, FeatureWeight[] allFeatures);
  
  protected FeatureWeight[] getWeights(Collection<Feature> features, IndexSearcher searcher) throws IOException {
    FeatureWeight[] arr = new FeatureWeight[features.size()];
    int i = 0;
    SolrQueryRequest req = this.getRequest();
    // since the feature store is a linkedhashmap order is preserved
    for (Feature f : features) {
      arr[i] = f.createWeight(searcher);
      arr[i].setRequest(req);
      arr[i].setOriginalQuery(originalQuery);

      ++i;
    }
    return arr;
  }
  
  @Override
  public String toString(String field) {
    return field;
  }
  
  public abstract class ModelWeight extends Weight {

    IndexSearcher searcher;
    // List of the model's features used for scoring. This is a subset of the
    // features used for logging.
    FeatureWeight[] modelFeatures; 
    float[] modelFeatureValuesNormalized;

    // List of all the feature values, used for both scoring and logging
    FeatureWeight[] allFeatureWeights; 
    float[] allFeatureValues;
    String[] allFeatureNames;
    
    
    

    /**
     * @return the allFeatureWeights
     */
    public FeatureWeight[] getAllFeatureWeights() {
      return allFeatureWeights;
    }

    /**
     * @return the allFeatureValues
     */
    public float[] getAllFeatureValues() {
      return allFeatureValues;
    }
    
    

    /**
     * @return the modelFeatureValuesNormalized
     */
    public float[] getModelFeatureValuesNormalized() {
      return modelFeatureValuesNormalized;
    }

    /**
     * @return the allFeatureNames
     */
    public String[] getAllFeatureNames() {
      return allFeatureNames;
    }



    public ModelWeight(IndexSearcher searcher, FeatureWeight[] modelFeatures, FeatureWeight[] allFeatures) {
      this.searcher = searcher;
      this.allFeatureWeights = allFeatures;
      this.modelFeatures = modelFeatures;
      this.modelFeatureValuesNormalized = new float[modelFeatures.length];
      this.allFeatureValues = new float[allFeatures.length];
      this.allFeatureNames = new String[allFeatures.length];

      for (int i = 0; i < allFeatures.length; ++i) {
        allFeatureNames[i] = allFeatures[i].getName();
      }
    }
    
    
    

    /**
     * Goes through all the stored feature values, and calculates the normalized
     * values for all the features that will be used for scoring.
     */
    public void normalize() {
      int pos = 0;
      for (FeatureWeight feature : modelFeatures) {
        Normalizer norm = feature.getNorm();
        modelFeatureValuesNormalized[pos] = norm.normalize(allFeatureValues[feature.getId()]);
        pos++;
      }
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      FeatureScorer[] featureScorers = new FeatureScorer[allFeatureValues.length];

      Explanation[] explanations = new Explanation[allFeatureValues.length];
      int index = 0;
      for (FeatureWeight feature : allFeatureWeights) {
        featureScorers[index] = feature.scorer(context, null);
        explanations[index++] = feature.explain(context, doc);
      }

      List<Explanation> featureExplanations = new ArrayList<>();
      for (FeatureWeight f : modelFeatures) {
        Normalizer n = f.getNorm();
        Explanation e = explanations[f.id];
        if (n != IdentityNormalizer.INSTANCE)
          e = n.explain(e);
        featureExplanations.add(e);
      }
      // TODO this calls twice the scorers, could be optimized.

      // index = 0;
      // for (FeatureWeight feature : modelFeatures) {
      // featureScorers[index++] = feature.scorer(context, null);
      // }
      //ModelScorer bs = new ModelScorer(this, featureScorers);
      ModelScorer bs = makeModelScorer(this, featureScorers);
      // diego: no need to advance, scorers here will be in the required
      // position
      bs.advance(doc);

      float finalScore = bs.score();

      return Model.this.explain(context, doc, finalScore, featureExplanations);

    }

    @Override
    public Query getQuery() {
      return Model.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return 1;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      for (FeatureWeight feature : allFeatureWeights) {
        feature.normalize(norm, topLevelBoost);
      }
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, PostingFeatures features, Bits acceptDocs) throws IOException {
      FeatureScorer[] featureScorers = new FeatureScorer[allFeatureWeights.length];
      for (int i = 0; i < allFeatureWeights.length; i++) {
        featureScorers[i] = allFeatureWeights[i].scorer(context, acceptDocs);
      }
      //return new ModelScorer(this, featureScorers);
      return makeModelScorer(this, featureScorers);
    }
    
    protected abstract ModelScorer makeModelScorer(ModelWeight weight, FeatureScorer[] featureScorers);

    public abstract class ModelScorer extends Scorer {

      protected final FeatureScorer[] allFeatureScorers;

      /** The document number of the current match. */
      protected int doc = -1;

      protected ModelScorer(Weight weight, FeatureScorer featureScorers[]) {
        super(weight);
        this.allFeatureScorers = featureScorers;
      }

      @Override
      public long cost() {
        throw new UnsupportedOperationException();
        /*long sum = 0;
        for (int i = 0; i < allFeatureScorers.length; i++) {
          sum += allFeatureScorers[i].cost();
        }
        return sum;*/
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int advance(int target) throws IOException {
        assert doc != NO_MORE_DOCS;
        doc = NO_MORE_DOCS;
        for (FeatureScorer scorer : allFeatureScorers) {
          doc = Math.min(doc, scorer.advance(target));
        }
        return doc;
      }

     
      @Override
      public int freq() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int nextDoc() throws IOException {
        throw new UnsupportedOperationException();
      }

    }

  }

}
