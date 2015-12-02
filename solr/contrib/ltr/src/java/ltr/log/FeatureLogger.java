package ltr.log;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FeatureLogger can be registered in a model and provide a strategy for logging
 * the feature values.
 */
public abstract class FeatureLogger<RECORD> {

  public enum Format {
    CSV, JSON
  }

  private static final Logger logger = LoggerFactory.getLogger(FeatureLogger.class);

  public FeatureLogger(int rerank) {
    docIdToFeatures = new HashMap<>(rerank);
  }

  private Map<Integer,RECORD> docIdToFeatures;

  /**
   * Log will be called every time that the model generates the feature values
   * for a document and a query.
   * 
   * @param docid
   *          Solr document id whose features we are saving
   * @param featureNames
   *          List of all the feature names we are logging
   * @param featureValues
   *          Parallel list to featureNames that stores all the unnormalized
   *          feature values
   *
   * @return true if the logger successfully logged the features, false
   *         otherwise.
   */

  public boolean log(int docid, String[] featureNames, float[] featureValues) {
    RECORD r = makeRecord(docid, featureNames, featureValues);
    if (r == null)
      return false;
    docIdToFeatures.put(docid, r);
    return true;
  }

  /**
   * returns a FeatureLogger that logs the features in output, using the format
   * specified in the 'format' param: - csv will log the features as a unique
   * string in csv format - json will log the features in a map in a
   * Map<feature_name,featurevalue> if format is null or empty, csv format will
   * be selected.
   * 
   * @param name
   *          the format to use
   * @param rerank
   *          how many documents are reranked
   * @return a feature logger for the format specified.
   */
  public static FeatureLogger<?> getFeatureLogger(Format format, int rerank) {
    if (format == null) {
      return new CSVFeatureLogger(rerank);
    }
    if (Format.CSV == format) {
      return new CSVFeatureLogger(rerank);
    }
    if (Format.JSON == format) {
      return new MapFeatureLogger(rerank);
    }
    logger.warn("unknown feature logger {}", format);
    return null;

  }

  public abstract RECORD makeRecord(int docid, String[] featureNames, float[] featureValues);

  /**
   * populate the document with its feature vector
   * 
   * @param docid
   *          Solr document id
   * @return String representation of the list of features calculated for docid
   */
  public RECORD getFeatureVector(int docid) {
    return docIdToFeatures.get(docid);
  }

  public static class MapFeatureLogger extends FeatureLogger<Map<String,Float>> {

    public MapFeatureLogger(int rerank) {
      super(rerank);
    }

    @Override
    public Map<String,Float> makeRecord(int docid, String[] featureNames, float[] featureValues) {
      Map<String,Float> hashmap =  new HashMap<>(featureValues.length);
      for (int i = 0; i < featureNames.length; i++){
            hashmap.put(featureNames[i], featureValues[i]);
      }
      return hashmap;
    }

  }

  public static class CSVFeatureLogger extends FeatureLogger<String> {
    StringBuilder sb = new StringBuilder(500);
    char keyValueSep = ':';
    char featureSep = ';';

    public CSVFeatureLogger(int rerank) {
      super(rerank);
    }

    public CSVFeatureLogger setKeyValueSep(char keyValueSep) {
      this.keyValueSep = keyValueSep;
      return this;
    }

    public CSVFeatureLogger setFeatureSep(char featureSep) {
      this.featureSep = featureSep;
      return this;
    }

    @Override
    public String makeRecord(int docid, String[] featureNames, float[] featureValues) {
      for (int i = 0; i < featureNames.length; i++) {
          sb.append(featureNames[i]).append(keyValueSep).append(featureValues[i]);
          sb.append(featureSep);
      }

      String features = (sb.length() == 0) ? "" : sb.substring(0, sb.length() - 1);
      sb.setLength(0);
      return features;
    }

  }

}
