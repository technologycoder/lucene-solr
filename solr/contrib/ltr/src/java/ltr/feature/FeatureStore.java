package ltr.feature;

import java.util.Collection;
import java.util.LinkedHashMap;

import ltr.ranking.Feature;
import ltr.util.FeatureException;

/**
 * A feature store will maintain the list of couples <feature name, feature instance> 
 * Each feature name has a unique name. 
 *  
 */
public class FeatureStore {
  // uses a LinkedHashMap to maintain feature-name -> Feature 
  // and remember the order of insertion of the features.
  private LinkedHashMap<String,Feature> store = new LinkedHashMap<>();
  private String storeName;
  
  /**
   * @return the storeName
   */
  public String getStoreName() {
    return storeName;
  }

  /**
   * @param storeName the storeName to set
   */
  public void setStoreName(String storeName) {
    this.storeName = storeName;
  }

  /**
   * Create a new feature store with the given name
   * @param storeName the name of this feature store
   */
  public FeatureStore(String storeName){
    this.storeName = storeName;
  }
  
  /**
   * Return the feature with the given name, if the feature is contained in this feature store,
   * otherwise throws a FeatureException
   * @param name - the name of the feature
   * @return the feature object implementing the logic for producing the feature
   * @throws FeatureException if the feature was not registered in this store
   */
  public Feature get(String name) throws FeatureException{
    if (! store.containsKey(name)){
      throw new FeatureException("missing feature "+name +". Store name was: " + storeName + "Possibly this feature exists in another context.");
    }
    return store.get(name);
  }
  
  /**
   * @return the size of this feature store
   */
  public int size() {
    return store.size();
  }


  /**
   * @param name the name of a feature
   * @return true if the feature is contained in this store
   */
  public boolean containsFeature(String name) {
    return store.containsKey(name);
  }


  /**
   * add the a feature to this store
   * @param feature the feature to add
   */
  public void add(Feature feature){
    store.put(feature.getName(), feature);
  }

  /**
   * @return all the features in this store, sorted by insertion order
   */
  public Collection<Feature> getFeatures() {
    return store.values();
  }
  


  /**
   * removes all the features registered in this feature store 
   */
  public void clear() {
     store.clear();
    
  }

}
