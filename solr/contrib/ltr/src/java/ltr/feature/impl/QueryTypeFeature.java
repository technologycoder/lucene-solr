package ltr.feature.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This feature will return the number of terms in a query referring to a
 * particular type. For example the query:
 * 
 * topic.US topic.UK person.OBAMA
 * 
 * has two terms referring to the type 'topic', and one referring to the type
 * person. This is done by specifying the 'querytype' in the feature parameters
 * (eg. "querytype":"topic"), the feature will extract the terms from the query
 * and check if they start with the querytype. It will return the number of
 * distinct terms for a querytype or -1 in case of errors.
 */
public class QueryTypeFeature extends Feature {

  private static final Logger logger = LoggerFactory.getLogger(QueryTypeFeature.class);

  String querytype = null;

  public QueryTypeFeature() {

  }

  public void init(String name, NamedParams params, int id) throws FeatureException {
    super.init(name, params, id);
    if (!params.containsKey(CommonLtrParams.QUERY_TYPE)) {
      throw new FeatureException("missing param " + CommonLtrParams.QUERY_TYPE);
    }
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher) throws IOException {
    this.querytype = (String) params.get(CommonLtrParams.QUERY_TYPE);
    return new QueryTypeWeight(searcher, name, params, norm, id);
  }

  @Override
  public String toString() {
    return "QueryTypeFeature [querytype:" + querytype + "]";
  }

  public class QueryTypeWeight extends FeatureWeight {

    Set<Term> terms = new HashSet<>();
    float queryTypeFreq = 0;

    public QueryTypeWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return QueryTypeFeature.this;
    }

    public void setOriginalQuery(Query originalQuery) {
      if (originalQuery != null) {
        originalQuery.extractTerms(terms);
        for (Term term : terms) {
          String text = term.text();
          if (text.startsWith(querytype))
            queryTypeFreq++;
        }
      } else {
        queryTypeFreq = -1;
      }
    };

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      return new QueryTypeScorer(this, context);
    }

    public class QueryTypeScorer extends FeatureScorer {

      AtomicReaderContext context = null;

      public QueryTypeScorer(FeatureWeight weight, AtomicReaderContext context) {
        super(weight);
        this.context = context;
      }

      @Override
      public float score() {
        return queryTypeFreq;
      }

      @Override
      public String toString() {
        return "QueryTypeScorer [name=" + name + " querytype=" + querytype + " ]";
      }

    }
  }

}
