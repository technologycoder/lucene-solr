package ltr.feature.io;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import ltr.feature.FeatureStore;
import ltr.ranking.Feature;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads a json-encoded feature store from a reader. A feature store will be
 * encoded as a json map that contains the version of the feature store and a
 * parameter 'features' containing the list of features. Each feature will
 * be encoded as a map containing the attributes of a feature (name, type,
 * and optional params).
 */
public class JsonFileFeatureLoader {

  private static final Logger logger = LoggerFactory
      .getLogger(JsonFileFeatureLoader.class);

  public JsonFileFeatureLoader() {}

  /**
   * Loads the json encoded features in the reader in the given feature store.
   *
   * @param reader
   *          a reader on the json encoded features
   * @param store
   *          the store where to load the features.
   * @throws IOException
   *           if an error occurs during the reading of the file
   */
  @SuppressWarnings("rawtypes")
  public void loadFeatures(final Reader reader, final FeatureStore store)
      throws IOException {
    final JSONParser parser = new JSONParser(reader);
    final Object b = ObjectBuilder.getVal(parser);
    if (!(b instanceof Map)) {
      return;
    }
    @SuppressWarnings("unchecked")
    final Map<String,Object> params = (Map) b;
    if (params.containsKey(CommonLtrParams.VERSION)) {
      final Object value = params.get(CommonLtrParams.VERSION);
      if (value instanceof Float) {
        store.setVersion((float) value);
      }
      if (value instanceof Double) {
        store.setVersion(((Double) value).floatValue());
      }
    }

    if (params.containsKey(CommonLtrParams.FEATURE_FIELD)) {
      final List<Object> featureList = (List) params
          .get(CommonLtrParams.FEATURE_FIELD);
      int featureId = 0;
      for (final Object o : featureList) {
        final Feature f = this.getFeature(o, featureId);
        if (f != null) {
          store.add(f);
          ++featureId;
        } else {
          logger.error("cannot load feature {}, store {}", o,
              store.getStoreName());
        }
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private Feature getFeature(final Object o, final int id) {
    if (!(o instanceof Map)) {
      return null;
    }
    final Map map = (Map) o;
    final String name = (String) map.get(CommonLtrParams.FEATURE_NAME);
    final String type = (String) map.get(CommonLtrParams.FEATURE_TYPE);

    NamedParams params = null;

    if (map.containsKey(CommonLtrParams.FEATURE_PARAMS)) {
      @SuppressWarnings("unchecked")
      final Map<String,Object> np = (Map<String,Object>) map
      .get(CommonLtrParams.FEATURE_PARAMS);
      params = new NamedParams(np);
    }

    try {
      return this.createFeature(name, type, params, id);
    } catch (final FeatureException e) {
      logger.error("loading feature: {} \n{} ",name, e);
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
  private Feature createFeature(final String name, final String type,
      final NamedParams params, final int id) throws FeatureException {
    try {
      final Class<?> c = Class.forName(type);
      final Feature f = (Feature) c.newInstance();
      f.init(name, params, id);
      return f;
    } catch (final Exception e) {
      logger.error("creating feature {} \n {}",name, e);
      throw new FeatureException(name, e);
    }
  }

}
