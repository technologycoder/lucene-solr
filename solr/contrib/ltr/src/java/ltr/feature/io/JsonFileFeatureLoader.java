package ltr.feature.io;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import ltr.feature.FeatureStore;
import ltr.ranking.Feature;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load a json-encoded feature store from a reader
 * 
 */
public class JsonFileFeatureLoader {

  private static final Logger logger = LoggerFactory.getLogger(JsonFileFeatureLoader.class);

  public JsonFileFeatureLoader() {
  }

  /**
   * Loads the json encoded features in the reader in the given feature store.
   * 
   * @param reader a reader on the json encoded features
   * @param store the store where to load the features.
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public void loadFeatures(Reader reader, FeatureStore store) throws IOException {
    JSONParser parser = new JSONParser(reader);
    Object b = ObjectBuilder.getVal(parser);
    if (!(b instanceof List)) {
      return;
    }
    @SuppressWarnings("unchecked")
    List<Object> list = (List) b;
    int id = 0;
    for (Object o : list) {
      Feature f = getFeature(o, id);
      if (f != null){
        store.add(f);
        ++id;
      } else {
        logger.error("cannot load feature {}, store {}", o, store.getStoreName());
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private Feature getFeature(Object o, int id) {
    if (!(o instanceof Map)){
      return null;
    }
    Map map = (Map) o;
    String name = (String) map.get("name");
    String type = (String) map.get("type");

    NamedParams params = null;

    if (map.containsKey("params")) {
      @SuppressWarnings("unchecked")
      Map<String,Object> np = (Map<String,Object>) map.get("params");
      params = new NamedParams(np);
    }

    try {
      return createFeature(name, type, params, id);
    } catch (FeatureException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  /**
   * generates an instance of the feature class specified in type.
   * 
   * @param name
   *          a symbolic name for the feature
   * @param type
   *          the full class name of the class implementing the feature
   * @param params
   *          input params for the feature
   * @param id
   *          - unique int identifier for the feature
   */
  private Feature createFeature(String name, String type, NamedParams params, int id) throws FeatureException {
    try {
      Class<?> c = Class.forName(type);
      Feature f = (Feature) c.newInstance();
      f.init(name, params, id);
      return f;
    } catch (Exception e) {
      throw new FeatureException(name, e);
    }
  }

}
