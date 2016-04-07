package ltr.feature.impl;

import java.io.IOException;
import java.util.Scanner;
import java.util.Set;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
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
 * Returns the number of companies per document, ignore fake codes
 */
public class CompaniesPerDocumentFeature extends Feature {
  private String field;

  public CompaniesPerDocumentFeature() {

  }

  @Override
  public void init(final String name, final NamedParams params, final int id,final float defaultValue) throws FeatureException {
    super.init(name, params, id, defaultValue);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      throw new FeatureException("missing param" + CommonLtrParams.FIELD);
    }
  }

  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher) throws IOException {
    this.field = (String) this.params.get(CommonLtrParams.FIELD);
    return new CompaniesPerDocumentFeatureWeight(searcher, this.name, this.params, this.norm, this.id);
  }

  @Override
  public String toString() {
    return "CompaniesPerDocumentFeature [field:" + this.field + "]";

  }

  public class CompaniesPerDocumentFeatureWeight extends FeatureWeight {

    public CompaniesPerDocumentFeatureWeight(final IndexSearcher searcher, final String name, final NamedParams params, final Normalizer norm, final int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return CompaniesPerDocumentFeature.this;
    }

    @Override
    public FeatureScorer scorer(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      return new CompaniesPerDocumentFeatureScorer(this, context);

    }

    public class CompaniesPerDocumentFeatureScorer extends FeatureScorer {

      private final AtomicReaderContext context;
      private final Set<String> fields = Sets.newHashSet();

      public CompaniesPerDocumentFeatureScorer(final FeatureWeight weight, final AtomicReaderContext context) throws IOException {
        super(weight);
        this.context = context;
        this.fields.add(CompaniesPerDocumentFeature.this.field);
      }

      @Override
      public float score() throws IOException {

        final Document doc = this.context.reader().document(this.docID, this.fields);
        final IndexableField[] indexFields = doc.getFields(CompaniesPerDocumentFeature.this.field);
        if (indexFields == null){
          return defaultValue;
        }
        int companies = 0;
        for (IndexableField indexField : indexFields){
          Scanner scanner = new Scanner(indexField.stringValue());
          scanner.useDelimiter(":");
          // companies are multivalued fields and each company is formatted using 
          // the following format:
          
          // tickername:assigned_score_c:assigned_score_n:derived_score_c:derived_score_n:[rollup_score_c:rollup_score_n]
          // 
          // example:
          // ALLBBUNDCOMPANIES:0:1:0:1:95:20
          // 
          // where *_c are the scores given by the classifier (from 1 to 100), 
          // while *_n are normalized to be used as term frequencies inside lucene (from 1 to 30)
          // since we could also have fake companies in the list, we filter them looking at derived_score_c:
          // if is 0 the record doesn't represent a real company
          for (int i = 0; i < 3; i++){
            // skip the first 3 fields
            if (! scanner.hasNext()) break;
            scanner.next();
          }
          
          if (scanner.hasNextInt()) {
            // derived_score_c
            if (scanner.nextInt() > 0) companies++;
          }
          scanner.close();
        }
        return companies;
      }

      @Override
      public String toString() {
        return "CompaniesPerDocumentFeature [name=" + this.name + " field=" + CompaniesPerDocumentFeature.this.field + "]";
      }

    }
  }

}
