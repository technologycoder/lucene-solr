package ltr.feature;

import ltr.TestRerankBase;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Diego Ceccarelli - dceccarelli4@bloomberg.net
 *
 * @since Nov 12, 2015
 */
public class TestFeatures extends TestRerankBase {

  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
    System.out.println("-----------index ---------------------");
    assertU(adoc("title", "bloomberg different bla", "description", "bloomberg", "id", "6", "popularity", "1"));
    assertU(adoc("title", "bloomberg bloomberg ", "description", "bloomberg", "id", "7", "popularity", "2"));
    assertU(adoc("title", "bloomberg bloomberg bloomberg", "description", "bloomberg", "id", "8", "popularity", "3"));
    assertU(adoc("title", "bloomberg bloomberg bloomberg bloomberg", "id", "9", "popularity", "5"));
    assertU(commit());
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }
  
  
  private SolrQuery getQuery(String query, String featureModel, int rows){
    SolrQuery q = new SolrQuery();
    q.setQuery(query);
    q.add("rows", String.valueOf(rows));
    // get the feature vector binding it to the field name fv
    q.add("fl", "*,score,fv:[fv model="+featureModel+"]");
    q.add("wt", "json");
    return q;
  }

  @Test
  public void testQueryTypeFeature() throws Exception {
    SolrQuery query = getQuery("TOPIC.test TOPIC.test1 OR *:*","test_query_type_feature",1);

    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='query_has_topic:2.0'");
  }
  
  
  @Test
  public void testQueryScore() throws Exception {
    SolrQuery query = getQuery("title:bloomberg","test_original_score_feature",4);
   
    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='original_score:0.7768564'");
    assertJQ("/query?" + query.toString(), "/response/docs/[3]/fv=='original_score:0.3884282'");
  }

  
  @Test
  public void testQueryFeatures() throws Exception {
    SolrQuery query = getQuery("title:bloomberg","query_features",4);
    
    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='query_has_topic:0.0;original_score:0.7768564'");
  }
  
  @Test
  public void testFieldLengthFeature() throws Exception {
    SolrQuery query = getQuery("*:*","test_field_length_feature",4);    
    
    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='title_length:4.0;description_length:1.0'");
  }
  
  
  @Test
  public void testFieldCharLengthFeature() throws Exception {
    SolrQuery query = getQuery("title:\"bloomberg different bla\"","test_field_char_length_feature",4);    
   
    float titleCharLength = "bloomberg different bla".length();
    float descriptionCharLength = "bloomberg".length();
    
    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='title_char_length:"+titleCharLength+";description_char_length:"+descriptionCharLength+"'");
  }
  
  
  
  @Test
  public void testFunctionQueryFeature() throws Exception {
    SolrQuery query = getQuery("popularity:3","test_function_query_feature",4);    
    
    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='powpolarity:9.0'");
    
    query.setQuery("popularity:5");
    
    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='powpolarity:25.0'");
  }
  
  @Test
  public void testConstantFeature() throws Exception {
    SolrQuery query = getQuery("*:*","test_constant_feature",1);    
 
    assertJQ("/query?"+query.toString(), "/response/docs/[0]/fv=='c1:42.0'");
  }
  
  
  
  @Test
  public void testHasFieldFeature() throws Exception {
    // this document doesn't have the field description
    SolrQuery query = getQuery("popularity:5","test_has_field_feature",4);    
    
    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='has_title:1.0;has_description:0.0'");
    
    query.setQuery("popularity:1");
    assertJQ("/query?" + query.toString(), "/response/docs/[0]/fv=='has_title:1.0;has_description:1.0'");
  }
  
  
  
  
  
  
  
  
}
