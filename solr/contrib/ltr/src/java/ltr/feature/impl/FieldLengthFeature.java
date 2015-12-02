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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SmallFloat;

import com.google.common.collect.Sets;
/**
 * This feature will return the length in terms of a field, base on its norm field.
 * If the norm is not available it will throw an exception when solr is started.
 * 
 */
public class FieldLengthFeature extends Feature {
  String field;

  /** Cache of decoded bytes. */

  private static final float[] NORM_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      NORM_TABLE[i] = SmallFloat.byte315ToFloat((byte) i);
    }
  }

  /**
   * Decodes the norm value, assuming it is a single byte.
   * 
   * @see #encodeNormValue(float)
   */

  private final float decodeNorm(long norm) {
    return NORM_TABLE[(int) (norm & 0xFF)]; // & 0xFF maps negative bytes to
    // positive above 127
  }

  public FieldLengthFeature() {

  }

  public void init(String name, NamedParams params, int id) throws FeatureException {
    super.init(name, params, id);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      throw new FeatureException("missing param "+CommonLtrParams.FIELD);
    }
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher) throws IOException {
    this.field = (String) params.get(CommonLtrParams.FIELD);
    return new FieldLengthFeatureWeight(searcher, name, params, norm, id);
  }

  @Override
  public String toString() {
    return "FieldLengthFeature [field:" + field + "]";

  }

  public class FieldLengthFeatureWeight extends FeatureWeight {

    public FieldLengthFeatureWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return FieldLengthFeature.this;
    }

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      return new FieldLengthFeatureScorer(this, context);

    }

    public class FieldLengthFeatureScorer extends FeatureScorer {

      AtomicReaderContext context = null;
      NumericDocValues norms = null;
      Set<String> fields = Sets.newHashSet();

      public FieldLengthFeatureScorer(FeatureWeight weight, AtomicReaderContext context) throws IOException {
        super(weight);
        this.context = context;
        fields.add(field);
        norms = context.reader().getNormValues(field);

        Document doc = searcher.doc(0);
        if (doc != null) {
          IndexableField idxF = doc.getField(field);

          if (idxF != null && idxF.fieldType().omitNorms()) {
            throw new IOException("FieldLengthFeatures can't be used if omitNorms is enabled (field=" + field + ")");
          }
        }
        // In the constructor, docId is -1, so using 0 as default lookup
        IndexableField idxF = searcher.doc(0).getField(field);
        if (idxF.fieldType().omitNorms()) {
          throw new IOException("FieldLengthFeatures can't be used if omitNorms is enabled (field=" + field + ")");
        }
      }

      @Override
      public float score() throws IOException {
        Document d = context.reader().document(docID, fields);
        IndexableField idxF = d.getField(field);
        float boost = idxF.boost();
        long encodedNorm = norms.get(docID);
        float norm = decodeNorm(encodedNorm);
        float numTerms = (float) Math.pow(boost / norm, 2);
        return numTerms;
      }

      @Override
      public String toString() {
        return "FieldLengthFeature [name=" + name + " field=" + field + "]";
      }

    }
  }

}
