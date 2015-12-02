package ltr.ranking;

import java.io.IOException;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.intervals.IntervalIterator;

/**
 * A 'recipe' for computing a feature 
 * @see Feature
 */
public abstract class FeatureScorer extends Scorer {

  protected int docID = -1;
  protected String name;

  public FeatureScorer(FeatureWeight weight) {
    super(weight);
    this.name = weight.getName();
  }

  @Override
  public abstract float score() throws IOException;

  /**
   * Used in the FeatureWeight's explain. Each feature should implement this
   * returning properties of the specific scorer useful for an explain. For
   * example "MyCustomClassFeature [name=" + name + "myVariable:" + myVariable +
   * "]";
   */
  @Override
  public abstract String toString();

  @Override
  public int freq() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int docID() {
    return docID;
  }
  
  public float getDefaultScore(){
    return Float.MAX_VALUE;
  }
  

  @Override
  public int nextDoc() throws IOException {
    // only the rescorer will call this scorers and nextDoc will never be called
    throw new UnsupportedOperationException();
  }

  @Override
  public int advance(int target) throws IOException {
    // For advanced features that use Solr scorers internally, 
    // you must override and pass this call on to them
    docID = target;
    return docID;
  }

  @Override
  public long cost() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public IntervalIterator intervals(boolean arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

}
