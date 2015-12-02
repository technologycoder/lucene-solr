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
 * This feature will return true if the current has the specified field, otherwise 
 * false.  
 */
public class HasFieldFeature extends Feature {
  String field;
  Set<String> fields = Sets.newHashSet();

  public HasFieldFeature() {

  }
  
  public void init(String name, NamedParams params, int id) throws FeatureException {
    super.init(name, params, id);
    if (! params.containsKey(CommonLtrParams.FIELD)){
      throw new FeatureException("missing param "+CommonLtrParams.FIELD);
    }
  }


  @Override
  public FeatureWeight createWeight(IndexSearcher searcher) throws IOException {
    this.field = (String) params.get(CommonLtrParams.FIELD);
    fields.add(this.field);
    return new HasFieldFeatureWeight(searcher, name, params, norm, id);
  }

  @Override
  public String toString() {
    return "DocValueFeature [field:" + field + "]";

  }

  public class HasFieldFeatureWeight extends FeatureWeight {

    public HasFieldFeatureWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return HasFieldFeature.this;
    }

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      return new HasFieldFeatureScorer(this, context);
    }

    
    public class HasFieldFeatureScorer extends FeatureScorer {

      AtomicReaderContext context = null;

      public HasFieldFeatureScorer(FeatureWeight weight, AtomicReaderContext context) {
        super(weight);
        this.context = context;
      }

      @Override
      public float score() {
        try {
          Document d = context.reader().document(docID, fields);
          IndexableField f = d.getField(field);
          if (f== null) return 0;
          if (f.stringValue().length() > 0) return 1;

        } catch (IOException e) {
          //FIXME log? 
        }
        return 0;
      }

      @Override
      public String toString() {
        return "HasFieldFeatureScorer [name=" + name + " field=" + fields + "]";
      }

    }
  }

}
