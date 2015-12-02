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

import com.google.common.collect.Sets;


/**
 * This feature will return the number of values in a specified multivalued field. 
 * Float.MAXVALUE in case of error. 
 */
public class MultivaluedFieldLengthFeature extends Feature {
  String field;

  public MultivaluedFieldLengthFeature() {

  }

  public void init(String name, NamedParams params, int id) throws FeatureException {
    super.init(name, params, id);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      throw new FeatureException("missing param" + CommonLtrParams.FIELD);
    }
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher) throws IOException {
    this.field = (String) params.get(CommonLtrParams.FIELD);
    return new FieldLengthFeatureWeight(searcher, name, params, norm, id);
  }

  @Override
  public String toString() {
    return "MultivaluedFieldLengthFeature [field:" + field + "]";

  }

  public class FieldLengthFeatureWeight extends FeatureWeight {

    public FieldLengthFeatureWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return MultivaluedFieldLengthFeature.this;
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
      }

      @Override
      public float score() throws IOException {

        Document doc = context.reader().document(docID, fields);
        IndexableField[] idxf = doc.getFields(field);
        if (idxf == null) {
          return getDefaultScore();
        }
        return idxf.length;
      }

      @Override
      public String toString() {
        return "MultivaluedFieldLengthFeature [name=" + name + " field=" + field + "]";
      }

    }
  }

}
