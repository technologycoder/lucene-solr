package ltr.ranking;

import java.io.IOException;

import ltr.feature.norm.Normalizer;
import ltr.feature.norm.impl.IdentityNormalizer;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/**
 * A 'recipe' for computing a feature.
 */
public abstract class Feature extends Query {
  // A feature will produce a score for each document given a query so 
  // it is considered a scoring function, and extends the lucene Query object. This will allow
  // as to reuse lucene/solr classes in order to compute particular features.

  protected String name;
  protected String type = this.getClass().getCanonicalName();
  protected Normalizer norm = IdentityNormalizer.INSTANCE;
  public int id;
  protected NamedParams params = NamedParams.EMPTY;

  /**
   * initialize a feature
   * @param name the symbolic name of the feature. 
   * @param params optional params used to compute the feature
   * @param id a unique integer id associated to the feature
   * @throws FeatureException if something fails during the initialization
   */
  public void init(String name, NamedParams params, int id) throws FeatureException {
    this.name = name;
    this.params = params;
    this.id = id;
  }

  public Feature() {

  }

  @Override
  public String toString(String field) {
    return toString();
  }

  public abstract FeatureWeight createWeight(IndexSearcher searcher) throws IOException;

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + id;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((params == null) ? 0 : params.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj))
      return false;
    Feature other = (Feature) obj;
    if (id != other.id)
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (params == null) {
      if (other.params != null)
        return false;
    } else if (!params.equals(other.params))
      return false;
    if (type == null) {
      if (other.type != null)
        return false;
    } else if (!type.equals(other.type))
      return false;
    return true;
  }

  /**
   * @return the type
   */
  public String getType() {
    return type;
  }

  @Override
  public String toString() {
    return "Feature [name=" + name + ", type=" + type + ", id=" + id + ", params=" + params + "]";
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the norm
   */
  public Normalizer getNorm() {
    return norm;
  }

  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * @return the params
   */
  public NamedParams getParams() {
    return params;
  }

  /**
   * set a normalizer for this feature, if a normalizer is registered
   * the value of the features will be normalized. 
   * @param norm
   */
  public void setNorm(Normalizer norm) {
    this.norm = norm;

  }
  
  


}
