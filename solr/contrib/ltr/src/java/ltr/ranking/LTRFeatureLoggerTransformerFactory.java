package ltr.ranking;

import java.io.IOException;
import java.util.List;

import ltr.feature.FeatureStore;
import ltr.feature.LocalFeatureStores;
import ltr.log.FeatureLogger;
import ltr.model.impl.LoggingModel;
import ltr.model.impl.Model;
import ltr.model.impl.Model.ModelWeight;
import ltr.util.FeatureException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Weight.PostingFeatures;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformContext;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.response.transform.TransformerWithContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transformer will take care to generate and append in the response the
 * features declared in the feature store of the current model. The class is
 * useful if you are not interested in the reranking (e.g., bootstrapping a
 * machine learning framework).
 */

public class LTRFeatureLoggerTransformerFactory extends TransformerFactory {

  private static final String MODEL_PARAM = "model";
  private static final String DEFAULT_MODEL = "nws";
  
  
  private static final Logger logger = LoggerFactory.getLogger(LTRFeatureLoggerTransformerFactory.class);

  LocalFeatureStores stores = LocalFeatureStores.getInstance();

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    super.init(args);
  }

  @Override
  public DocTransformer create(String name, SolrParams params, SolrQueryRequest req) {
    IndexSearcher searcher = req.getSearcher();
    String modelName = params.get(MODEL_PARAM, DEFAULT_MODEL);
    Model model = null;
    try {
      FeatureStore fs = stores.getStore(modelName);
      // I just need to log the feature vector, logging model will only compute them without 
      // performing any reranking
      model = new LoggingModel(fs);
    } catch (FeatureException e) {
      logger.error("retrieving the model {} ", modelName);
      e.printStackTrace();
      return null;
    }
    return new FeatureTransformer(name, searcher, model);
  }

  class FeatureTransformer extends TransformerWithContext {

    String name;
    List<AtomicReaderContext> leafContexts;
    Model reRankModel;
    ModelWeight modelWeight;
    FeatureLogger<?> featurelLogger;

    /**
     * @param name
     *          Name of the field to be added in a document representing the
     *          feature vectors
     */
    public FeatureTransformer(String name, IndexSearcher searcher, Model model) {
      this.name = name;
      this.reRankModel = model;

    }

    @Override
    public String getName() {
      return name;
    }

    public void setContext(TransformContext context) {
      super.setContext(context);
      if (context == null) {
        return;
      }
      if (context.req == null) {
        return;
      }

      if (reRankModel == null) {
        throw new SolrException(org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST, "model is null");
      }
      reRankModel.setRequest(context.req);
      reRankModel.setOriginalQuery(context.query);
      logger.info("query = {} ", context.query);
      featurelLogger = reRankModel.getFeatureLogger();
      IndexSearcher searcher = context.searcher;
      if (searcher == null){
        throw new SolrException(org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST, "searcher is null");
      }
      leafContexts = searcher.getTopReaderContext().leaves();
      Weight w;
      try {
        w = reRankModel.createWeight(searcher);
      } catch (IOException e) {
        // FIXME throw exception?
        throw new SolrException(ErrorCode.BAD_REQUEST, "error logging the features");
      }
      if (w == null || !(w instanceof ModelWeight)) {
        // FIXME throw exception?
        throw new SolrException(ErrorCode.BAD_REQUEST, "error logging the features");
      }
      modelWeight = (ModelWeight) w;
    }

    @Override
    public void transform(SolrDocument doc, int docid) throws IOException {

      int n = ReaderUtil.subIndex(docid, leafContexts);
      final AtomicReaderContext atomicContext = leafContexts.get(n);
      int deBasedDoc = docid - atomicContext.docBase;
      Scorer r = modelWeight.scorer(atomicContext, PostingFeatures.DOCS_AND_FREQS, null);

      if (r.advance(deBasedDoc) != deBasedDoc) {
        logger.info("cannot find doc {} = {}", docid, doc);
        doc.addField(name, featurelLogger.makeRecord(docid, new String[0], new float[0]));
      } else {
        float finalScore = r.score();
        String[] names = modelWeight.getAllFeatureNames();
        float[] values = modelWeight.getAllFeatureValues();
        doc.addField(name, featurelLogger.makeRecord(docid, names, values));
      }

    }

  }
}
