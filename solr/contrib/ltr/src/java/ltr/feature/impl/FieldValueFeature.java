package ltr.feature.impl;

import java.io.IOException;
import java.util.Set;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;


/**
 * This feature will return a numerical/boolean value contained in a document field.
 * If the value is not available it will return a default score
 * (Float.MAX_VALUE).
 */
public class FieldValueFeature extends Feature {
  String field;
  
  
  private static final Logger logger = LoggerFactory.getLogger(FieldValueFeature.class);

  public FieldValueFeature() {

  }

  public void init(String name, NamedParams params, int id) throws FeatureException {
    super.init(name, params, id);
    field = (String) params.get(CommonLtrParams.FIELD);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      logger.error("no field {} in the feature declaration", CommonLtrParams.FIELD);
      throw new FeatureException("missing param " + CommonLtrParams.FIELD);
    }
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher) throws IOException {

    return new FieldValueFeatureWeight(searcher, name, params, norm, id);
  }

  @Override
  public String toString() {
    return "DocValueFeature [field:" + field + "]";

  }

  public class FieldValueFeatureWeight extends FeatureWeight {

    Set<String> fields = Sets.newHashSet();

    public FieldValueFeatureWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
      fields.add(field);
    }

    @Override
    public Query getQuery() {
      return FieldValueFeature.this;
    }

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      return new FieldValueFeatureScorer(this, context);
    }

    public class FieldValueFeatureScorer extends FeatureScorer {

      AtomicReaderContext context = null;

      public FieldValueFeatureScorer(FeatureWeight weight, AtomicReaderContext context) {
        super(weight);
        this.context = context;
      }

      @Override
      public float score() {
        try {
          Document d = context.reader().document(docID, fields);
          IndexableField f = d.getField(field);
          if (f == null) {
            logger.debug("no field {}", f);
            return getDefaultScore();
          }
          Number number = f.numericValue();
          if (number != null) {
            return number.floatValue();
          } else {
            String string = f.stringValue();
            // boolean values in the index are encoded with the chars T/F
            if (string.equals("T")){
              return 1;
            }
            if (string.equals("F")){
              return 0;
            }
          }
        } catch (IOException e) {
        }
        logger.debug("cannot convert to feature field {}", field);
        return getDefaultScore();
      }

      @Override
      public String toString() {
        return "FieldValueFeature [name=" + name + " fields=" + fields + "]";
      }

    }

  }

}
