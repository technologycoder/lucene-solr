package ltr.feature.impl;

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * This feature will return the 'age' of a story. Given a field contained a date 
 * (could be the last-update, or the create date) the feature will contained the time
 * elapsed from the field date to the request date. You can get the delta with different
 * granularities (specified in the field 'granularity' (seconds, minutes, hours, days)).
 */
public class StoryAgeFeature extends Feature {

  private static final Logger logger = LoggerFactory.getLogger(StoryAgeFeature.class);

  String field;
  Set<String> fields = Sets.newHashSet();

  enum Granularity {
    seconds, minutes, hours, days
  }

  Granularity granularity;

  public StoryAgeFeature() {

  }

  public void init(String name, NamedParams params, int id) throws FeatureException {
    super.init(name, params, id);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      throw new FeatureException("missing param "+CommonLtrParams.FIELD);
    }
    if (!params.containsKey("granularity")) {
      throw new FeatureException("missing param granularity");
    }
    try {
      String strGranularity = (String) params.get("granularity");
      granularity = Granularity.valueOf(strGranularity);

    } catch (IllegalArgumentException | NullPointerException e) {
      logger.error("invalid or missing value for field granularity, accepted values are: seconds, minutes, hours, and days");
      throw new FeatureException("invalid or missing value for field granularity, accepted values are: seconds, minutes, hours, and days", e);
    }
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher) throws IOException {
    this.field = (String) params.get(CommonLtrParams.FIELD);
    fields.add(this.field);
    return new StoryAgeWeight(searcher, name, params, norm, id);
  }

  @Override
  public String toString() {
    return "DocValueFeature [field:" + field + "]";

  }

  public class StoryAgeWeight extends FeatureWeight {

    public StoryAgeWeight(IndexSearcher searcher, String name, NamedParams params, Normalizer norm, int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return StoryAgeFeature.this;
    }

    @Override
    public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      return new StoryAgeScorer(this, context);
    }

    public class StoryAgeScorer extends FeatureScorer {

      AtomicReaderContext context = null;
      Date date = new Date();

      public StoryAgeScorer(FeatureWeight weight, AtomicReaderContext context) {
        super(weight);
        this.context = context;
      }

      @Override
      public float score() {
        try {
          AtomicReader reader = context.reader();
          if (reader == null){
            return Float.MAX_VALUE;
          }
          Document d = context.reader().document(docID, fields);
          if (d == null){
            //FIXME log? 
            return getDefaultScore();
          }
          IndexableField f = d.getField(field);
          if (f == null){
            //FIXME log? 
            return getDefaultScore();
          }
          long ldate = f.numericValue().longValue();
          
          long current = request.getStartTime();
          long delta = current - ldate;
          switch (granularity) {
          case seconds:
            return TimeUnit.MILLISECONDS.toSeconds(delta);
          case minutes:
            return TimeUnit.MILLISECONDS.toMinutes(delta);
          case hours:
            return TimeUnit.MILLISECONDS.toHours(delta);
          case days:
            return TimeUnit.MILLISECONDS.toDays(delta);
          }

        } catch (IOException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, e);
        }
        // should not happen
        return getDefaultScore();
      }

      @Override
      public String toString() {
        return "StoryAgeScorer [name=" + name + " fields=" + fields + " granularity=" + granularity + " ]";
      }

    }
  }

}
