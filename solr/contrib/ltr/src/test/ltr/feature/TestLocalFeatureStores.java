package ltr.feature;

import ltr.TestRerankBase;

import org.apache.solr.core.SolrCore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLocalFeatureStores extends TestRerankBase {
  SolrCore solrCore;
  LocalFeatureStores stores;
  
  @Before
  public void before() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    this.solrCore = h.getCore();
    this.stores = new LocalFeatureStores();
  }
  
  @After
  public void after() throws Exception {
    h.close();
  }
  

  @Test
  public void testThatAFileInResourcesIsLoaded() throws Exception {
    // it will try to load the features store encoded in
    // test-constant-feature.json inside the
    // solr configuration directory (in case solr/collection1/conf/features, in the
    // test-files). test-constant-feature.json it will not find it, and it will
    // load it from the resources
    final FeatureStore fs = this.stores.getFeatureStoreFromSolrConfigOrResources(
        "test_constant_feature", this.solrCore.getResourceLoader());
    assertFalse(fs.isEmpty());
  }
  
  @Test
  public void testFeatureVersion() throws Exception {
    FeatureStore fs = this.stores.getFeatureStoreFromSolrConfigOrResources(
        "test_constant_feature", this.solrCore.getResourceLoader());
    assertEquals(1.0, fs.getVersion(), 0.000001);
    fs = this.stores.getFeatureStoreFromSolrConfigOrResources(
        "test_constant_feature_version2", this.solrCore.getResourceLoader());
    assertEquals(2.0, fs.getVersion(), 0.000001);
  }
  
  @Test
  public void testMissingFeatureStoreHasNegativeVersion() throws Exception {
    
    final FeatureStore fs = this.stores
        .getFeatureStoreFromSolrConfigOrResources("missing_feature_store",
            this.solrCore.getResourceLoader());
    assertEquals(-1.0, fs.getVersion(), 0.000001);
  }
}
