package org.apache.solr.ltr.rest;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.FeatureStore;
import org.apache.solr.ltr.feature.LTRScoringAlgorithm;
import org.apache.solr.ltr.feature.ModelStore;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.feature.norm.impl.IdentityNormalizer;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.util.CommonLTRParams;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.ModelException;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Menaged resource for storing a model
 */
public class ManagedModelStore extends ManagedResource implements
    ManagedResource.ChildResourceSupport {

  ModelStore store;
  private ManagedFeatureStore featureStores;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public ManagedModelStore(String resourceId, SolrResourceLoader loader,
      StorageIO storageIO) throws SolrException {
    super(resourceId, loader, storageIO);

    store = new ModelStore();

  }

  public void init(ManagedFeatureStore featureStores) {
    log.info("INIT model store");
    this.featureStores = featureStores;
  }

  private Object managedData;

  @SuppressWarnings("unchecked")
  @Override
  protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs,
      Object managedData) throws SolrException {
    store.clear();
    // the managed models on the disk or on zookeeper will be loaded in a lazy
    // way, since we need to set the managed features first (unfortunately
    // managed resources do not
    // decouple the creation of a managed resource with the reading of the data
    // from the storage)
    this.managedData = managedData;

  }

  public void loadStoredModels() {
    log.info("------ managed models ~ loading ------");

    if ((managedData != null) && (managedData instanceof List)) {
      final List<Map<String,Object>> up = (List<Map<String,Object>>) managedData;
      for (final Map<String,Object> u : up) {
        try {
          update(u);
        } catch (final ModelException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, e);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Feature parseFeature(Map<String,Object> featureMap,
      FeatureStore featureStore) throws FeatureException,
      CloneNotSupportedException {
    // FIXME name shouldn't be be null, exception?
    final String name = (String) featureMap.get(CommonLTRParams.FEATURE_NAME);

    final Normalizer norm;
    final Object normObj = featureMap.get(CommonLTRParams.FEATURE_NORM);
    if (normObj != null) {
      norm = Normalizer.fromMap(solrResourceLoader,
          (Map<String,Object>) normObj);
    } else {
      norm = IdentityNormalizer.INSTANCE;
    }

    if (featureStores == null) {
      throw new FeatureException("missing feature store");
    }

    Feature meta = featureStore.get(name);
    if (meta == null) {
      throw new FeatureException("feature " + name
          + " not found in store " + featureStore.getName());
    }

    meta = (Feature) meta.clone();
    meta.setNorm(norm);

    return meta;
  }

  @SuppressWarnings("unchecked")
  public LTRScoringAlgorithm makeLTRScoringAlgorithm(String json)
      throws ModelException {
    Object parsedJson = null;
    try {
      parsedJson = ObjectBuilder.fromJSON(json);
    } catch (final IOException ioExc) {
      throw new ModelException("ObjectBuilder failed parsing json", ioExc);
    }
    return makeLTRScoringAlgorithm((Map<String,Object>) parsedJson);
  }

  @SuppressWarnings("unchecked")
  public LTRScoringAlgorithm makeLTRScoringAlgorithm(Map<String,Object> map)
      throws ModelException {
    final String name = (String) map.get(CommonLTRParams.MODEL_NAME);
    final Object modelStoreObj = map.get(CommonLTRParams.MODEL_FEATURE_STORE);
    final String featureStoreName = (modelStoreObj == null) ? CommonLTRParams.DEFAULT_FEATURE_STORE_NAME
        : (String) modelStoreObj;
    NamedParams params = null;
    final FeatureStore fstore = featureStores.getFeatureStore(featureStoreName);
    if (!map.containsKey(CommonLTRParams.MODEL_FEATURE_LIST)) {
      // check if the model has a list of features to be used for computing the
      // ranking score
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Missing mandatory field features");
    }
    final List<Object> featureList = (List<Object>) map
        .get(CommonLTRParams.MODEL_FEATURE_LIST);
    final List<Feature> features = new ArrayList<>();
    for (final Object modelFeature : featureList) {
      try {
        // check the declared features exist in the feature store
        final Feature feature = parseFeature((Map<String,Object>) modelFeature,
            fstore);
        if (!fstore.containsFeature(feature.getName())) {
          throw new ModelException("missing feature " + feature.getName()
              + " in model " + name);
        }
        features.add(feature);
      } catch (FeatureException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      } catch (final CloneNotSupportedException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
    }

    if (map.containsKey(CommonLTRParams.MODEL_PARAMS)) {
      final Map<String,Object> paramsMap = (Map<String,Object>) map
          .get(CommonLTRParams.MODEL_PARAMS);
      params = new NamedParams(paramsMap);
    }

    final String type = (String) map.get(CommonLTRParams.MODEL_TYPE);
    LTRScoringAlgorithm meta = null;
    try {
      // create an instance of the model
      meta = solrResourceLoader.newInstance(
          type,
          LTRScoringAlgorithm.class,
          new String[0], // no sub packages
          new Class[] { String.class, List.class, String.class, List.class, NamedParams.class },
          new Object[] { name, features, featureStoreName, fstore.getFeatures(), params });
    } catch (final Exception e) {
      throw new ModelException("Model type does not exist " + type, e);
    }

    return meta;
  }

  @SuppressWarnings("unchecked")
  private void update(Map<String,Object> map) throws ModelException {

    final LTRScoringAlgorithm meta = makeLTRScoringAlgorithm(map);
    try {
      addMetadataModel(meta);
    } catch (final ModelException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Object applyUpdatesToManagedData(Object updates) {
    if (updates instanceof List) {
      final List<Map<String,Object>> up = (List<Map<String,Object>>) updates;
      for (final Map<String,Object> u : up) {
        try {
          update(u);
        } catch (final ModelException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, e);
        }
      }
    }

    if (updates instanceof Map) {
      final Map<String,Object> map = (Map<String,Object>) updates;
      try {
        update(map);
      } catch (final ModelException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
    }

    return store.modelAsManagedResources();
  }

  @Override
  public void doDeleteChild(BaseSolrResource endpoint, String childId) {
    // FIXME: hack to delete all the stores
    if (childId.equals("*")) {
      store.clear();
    }
    if (store.containsModel(childId)) {
      store.delete(childId);
    }
  }

  /**
   * Called to retrieve a named part (the given childId) of the resource at the
   * given endpoint. Note: since we have a unique child managed store we ignore
   * the childId.
   */
  @Override
  public void doGet(BaseSolrResource endpoint, String childId) {

    final SolrQueryResponse response = endpoint.getSolrResponse();
    response.add(CommonLTRParams.MODELS_JSON_FIELD,
        store.modelAsManagedResources());

  }

  public synchronized void addMetadataModel(LTRScoringAlgorithm modeldata)
      throws ModelException {
    log.info("adding model {}", modeldata.getName());
    store.addModel(modeldata);
  }

  public LTRScoringAlgorithm getModel(String modelName) {
    // this function replicates getModelStore().getModel(modelName), but
    // it simplifies the testing (we can avoid to mock also a ModelStore).
    return store.getModel(modelName);
  }

  public ModelStore getModelStore() {
    return store;
  }

  @Override
  public String toString() {
    return "ManagedModelStore [store=" + store + ", featureStores="
        + featureStores + "]";
  }

}