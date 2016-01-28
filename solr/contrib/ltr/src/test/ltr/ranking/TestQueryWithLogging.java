package ltr.ranking;

import ltr.TestRerankBase;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Diego Ceccarelli - dceccarelli4@bloomberg.net
 *
 * @since  Nov 5, 2015
 */
public class TestQueryWithLogging extends TestRerankBase {
  
  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
    bulkIndex();
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

 
  
  @Test
  public void testMissingFeatureStore() throws Exception {
    SolrQuery query = new SolrQuery();
    
    query.setQuery("*:*");
    query.add("rows", "1");
    // get the feature vector binding it to the field name fv
    query.add("fl", "*,score,fv:[fv model=missing]");
    query.add("wt", "json");
 
    assertJQ("/query?"+query.toString(), "/response/docs/[0]/fv=='@missing:-1.0'");
  }
  
  @Test
  public void testForEachDocumentTheCorrectFeatureValueForAFeatureIsReturned() throws Exception {
    SolrQuery query = new SolrQuery();
    
    query.setQuery("*:*");
    query.add("rows", "4");
    // get the feature vector binding it to the field name fv
    query.add("fl", "*,score,fv:[fv model=test_different_feature_values]");
    query.add("wt", "json");
 
    assertJQ("/query?"+query.toString(), "/response/docs/[0]/fv=='@test_different_feature_values:1.0;popularity:1.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[1]/fv=='@test_different_feature_values:1.0;popularity:2.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[2]/fv=='@test_different_feature_values:1.0;popularity:3.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[3]/fv=='@test_different_feature_values:1.0;popularity:5.0'");
  }

  
  @Test
  public void testForEachDocumentMultipleFeaturesAreReturned() throws Exception {
    SolrQuery query = new SolrQuery();
    
    query.setQuery("*:*");
    query.add("rows", "4");
    // get the feature vector binding it to the field name fv
    query.add("fl", "*,score,fv:[fv model=test_multiple_feature_values]");
    query.add("wt", "json");
 
    assertJQ("/query?"+query.toString(), "/response/docs/[0]/fv=='@test_multiple_feature_values:1.0;popularity:1.0;c1:42.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[1]/fv=='@test_multiple_feature_values:1.0;popularity:2.0;c1:42.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[2]/fv=='@test_multiple_feature_values:1.0;popularity:3.0;c1:42.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[3]/fv=='@test_multiple_feature_values:1.0;popularity:5.0;c1:42.0'");
  }

  
  @Test
  public void testEmptyFeatureStore() throws Exception {
    SolrQuery query = new SolrQuery();
    
    query.setQuery("*:*");
    query.add("rows", "4");
    // get the feature vector binding it to the field name fv
    query.add("fl", "*,score,fv:[fv model=empty]");
    query.add("wt", "json");
 
    assertJQ("/query?"+query.toString(), "/response/docs/[0]/fv=='@empty:1.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[1]/fv=='@empty:1.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[2]/fv=='@empty:1.0'");
    assertJQ("/query?"+query.toString(), "/response/docs/[3]/fv=='@empty:1.0'");
  }
  
 
  
  
  
  

}
