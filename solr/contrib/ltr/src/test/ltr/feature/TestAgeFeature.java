package ltr.feature;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class TestAgeFeature extends TestRerankBase {

  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
    System.out.println("-----------index ---------------------");
    assertU(adoc("id","1","date", "1972-05-20T17:33:18.772Z"));
    
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
  
  private float getFeatureValue(String response, String name){
    Pattern p = Pattern.compile(name+":([^;\"]*)");
    Matcher m = p.matcher(response);
    float value = -1;
    if (m.find()) {
        String str = m.group(1);
        value = Float.valueOf(str);   
    }
    return value;
  }

  @Test
  public void testAgeFeature() throws Exception {
    SolrQuery query = getQuery("*:*","test_story_age_feature",1);
    String response = restTestHarness.query("/query?" + query.toString());
    float hours = getFeatureValue(response, "story_age_hours");
    float days = getFeatureValue(response, "story_age_days");
    float minutes = getFeatureValue(response, "story_age_minutes");
    float seconds = getFeatureValue(response, "story_age_seconds");
    assertTrue(seconds > minutes);
    assertTrue(minutes > hours);
    assertTrue(hours > days);
    

  }  
  
}
