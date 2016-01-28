package ltr.ranking;

import java.io.IOException;
import java.util.Map;

import ltr.feature.norm.Normalizer;
import ltr.feature.norm.impl.IdentityNormalizer;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.request.SolrQueryRequest;

public abstract class FeatureWeight extends Weight implements Cloneable {
  
  protected String name;
  protected NamedParams params = NamedParams.EMPTY;
  protected Normalizer norm = IdentityNormalizer.INSTANCE;
  protected IndexSearcher searcher;
  protected SolrQueryRequest request;
  protected Map<String,String> efi;
  protected Query originalQuery;
  public int id;
  
  /**
   * Initialize a feature without the normalizer from the feature file. This is
   * called on initial construction since multiple models share the same
   * features, but have different normalizers. A concrete model's feature is
   * copied through featForNewModel().
   *
   * @param searcher
   *          Solr searcher available for features if they need them
   *
   * @param name
   *          Name of the feature
   * @param params
   *          Custom parameters that the feature may use
   * @param norm
   *          Feature normalizer used to normalize the feature value
   * @param id
   *          Unique ID for this feature. Similar to feature name, except it can
   *          be used to directly access the feature in the global list of
   *          features.
   */
  public FeatureWeight(final IndexSearcher searcher, final String name,
      final NamedParams params, final Normalizer norm, final int id) {
    this.searcher = searcher;
    this.name = name;
    this.params = params;
    this.id = id;
    this.norm = norm;
  }
  
  public final void setRequest(final SolrQueryRequest request) {
    this.request = request;
  }
  
  public final void setExternalFeatureInfo(final Map<String,String> efi) {
    this.efi = efi;
  }
  
  /**
   * Called once after all parameters have been set on the weight. Override this
   * to do things with the original query, request, or external parameters.
   *
   * @throws IOException
   */
  public void process() throws IOException {}
  
  public String getName() {
    return this.name;
  }
  
  public Normalizer getNorm() {
    return this.norm;
  }
  
  public NamedParams getParams() {
    return this.params;
  }
  
  public int getId() {
    return this.id;
  }
  
  @Override
  public Scorer scorer(final AtomicReaderContext context,
      final PostingFeatures arg1, final Bits acceptDocs) throws IOException {
    return this.scorer(context, acceptDocs);
  }
  
  public abstract FeatureScorer scorer(AtomicReaderContext context,
      Bits acceptDocs) throws IOException;
  
  @Override
  public Explanation explain(final AtomicReaderContext context, final int doc)
      throws IOException {
    final FeatureScorer r = this.scorer(context, null);
    r.advance(doc);
    float score = r.getDefaultScore();
    if (r.docID() == doc) {
      score = r.score();
    }
    final Explanation e = new Explanation(score, r.toString());
    return e;
  }
  
  @Override
  public float getValueForNormalization() throws IOException {
    return 1f;
  }
  
  @Override
  public void normalize(final float norm, final float topLevelBoost) {
    // For advanced features that use Solr weights internally, you must override
    // and pass this call on to them
  }
  
  @Override
  public String toString() {
    return this.getClass().getName() + " [name=" + this.name + ", params="
        + this.params + "]";
  }
  
  /**
   * @param originalQuery
   *          the originalQuery to set
   * @throws IOException
   *           this method could be used to manipulate the query and could throw
   *           IOExceptions
   */
  public void setOriginalQuery(final Query originalQuery) throws IOException {
    this.originalQuery = originalQuery;
  }
  
  /**
   * Default FeatureScorer class that returns the score passed in. Can be used
   * as a simple ConstantFeature, or to return a default scorer in case an
   * underlying feature's scorer is null.
   */
  public class ConstantFeatureScorer extends FeatureScorer {
    
    float constScore;
    String featureType;
    
    public ConstantFeatureScorer(final FeatureWeight weight,
        final float constScore, final String featureType) {
      super(weight);
      this.constScore = constScore;
      this.featureType = featureType;
    }
    
    @Override
    public float score() {
      return this.constScore;
    }
    
    @Override
    public String toString() {
      return this.featureType + " [name=" + this.name + " value="
          + this.constScore + "]";
    }
    
  }
  
}
