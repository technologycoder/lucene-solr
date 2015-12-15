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

import com.google.common.collect.Sets;
/**
 * This feature will return the character length of a stored field.
 * If the field is not available it will return a default score
 * (Float.MAX_VALUE).
 * 
 */
public class FieldCharLengthFeature extends Feature {
  String field;

  public FieldCharLengthFeature() {

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
    return new FieldCharLengthFeatureWeight(searcher, name, params, norm, id);
  }

  @Override
  public String toString() {
    return "FieldLengthFeature [field:" + field + "]";
  }

  public class FieldCharLengthFeatureWeight extends FeatureWeight {
    Set<String> fields = Sets.newHashSet();
    
    public FieldCharLengthFeatureWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
      fields.add(field);
    }

    @Override
    public Query getQuery() {
      return FieldCharLengthFeature.this;
    }

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      return new FieldCharLengthFeatureScorer(this, context);

    }

    public class FieldCharLengthFeatureScorer extends FeatureScorer {

      AtomicReaderContext context = null;
 

      public FieldCharLengthFeatureScorer(FeatureWeight weight, AtomicReaderContext context) throws IOException {
        super(weight);
        this.context = context;
      }

      @Override
      public float score() throws IOException {
        Document doc = context.reader().document(docID,fields);
        if (doc != null) {
          IndexableField idxF = doc.getField(field);
          if (idxF != null){
            String value = idxF.stringValue();
            if (value != null) return value.length();
          }
        }
        return getDefaultScore();
      }

      @Override
      public float getDefaultScore() {
    	  return 0;
      }
      
      @Override
      public String toString() {
        return "FieldCharLengthFeature [name=" + name + " field=" + field + "]";
      }

    }
  }
}
