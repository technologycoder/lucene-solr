package ltr.feature;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import ltr.feature.io.JsonFileFeatureLoader;
import ltr.util.FeatureException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local feature stores are loaded from the resources. 
 * Each feature store will be encoded in one json file, where the name of 
 * the file will match the name of the feature store (e.g., nws.json 
 * will contain the features for the feature store 'nws').
 * 
 * Local feature stores will be located in the folder '$LOCAL_FEATURE_FOLDER' 
 * inside src/resources
 * 
 */
public class LocalFeatureStores {

  private final static String LOCAL_FEATURE_FOLDER = "features";
  
  private Map<String,FeatureStore> stores = null;
  private JsonFileFeatureLoader loader = null;
  
  private static final Logger logger = LoggerFactory.getLogger(LocalFeatureStores.class);
  private final static LocalFeatureStores instance = new LocalFeatureStores();

  private LocalFeatureStores() {
    stores = new HashMap<>();
    loader = new JsonFileFeatureLoader();
  }

  public static LocalFeatureStores getInstance() {
    return instance;
  }

  public FeatureStore getStore(String name) throws FeatureException {
    return getStore("/"+LOCAL_FEATURE_FOLDER, name);
  }

  public synchronized FeatureStore getStore(String resourcePath, String name) throws FeatureException {

    if (stores.containsKey(name)){
      return stores.get(name);
    }
    FeatureStore f = load(resourcePath, name);
    logger.debug("loading model in {} {} = ", resourcePath, name);
    logger.debug("model = {}", f.getFeatures());
    stores.put(name, f);
    return f;
  }

  private FeatureStore load(String resourcePath, String name) throws FeatureException {
    FeatureStore store = new FeatureStore(name);
    
    String resource = resourcePath + "/" + name + ".json";

    InputStream stream = getClass().getResourceAsStream(resource);
    if (stream == null) {
      logger.error("can not find local feature store in {}", resource);
      // returns an empty store
      return store;
    }
    Reader r = new InputStreamReader(stream);
    try {
      loader.loadFeatures(r, store);
    } catch (IOException e) {
      logger.error("loading the resource {}", resource);
    }
    try {
      r.close();
    } catch (IOException e) {
      logger.error("loading the resource {}", resource);
    }
    return store;
  }

}
