package ltr.feature;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ltr.TestRerankBase;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCompaniesPerDocumentFeature extends TestRerankBase {

  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
    System.out.println("-----------index ---------------------");
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField("id", 1);
    doc1.addField("ticker", new String[] { 
          "ALLBBUNDCOMPANIES:0:1:0:1:95:20",
          "COUNTRY1:0:1:0:1:95:20",
          "DOMICILE:0:1:0:1:95:20",
          "IBM:95:20:95:20:95:20",
          "MARKETCAP:0:1:0:1:95:20",
          "MKTCAPOVER50BLN:0:1:0:1:95:20"});
    
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", 2);
    doc2.addField("ticker", new String[] { 
          "ALLBBUNDCOMPANIES:0:1:0:1:95:20",
          "COUNTRY1:0:1:0:1:95:20",
          "DOMICILE:0:1:0:1:95:20",
          "MARKETCAP:0:1:0:1:95:20",
          "MKTCAPOVER50BLN:0:1:0:1:95:20"});
    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField("id", 3);
    doc3.addField("ticker", new String[] { 
          "IBM:95:20:95:20:95:20",
          "FB:95:20:1:20:95:20",
          "ALLBBUNDCOMPANIES:0:1:0:1:95:20",
          "COUNTRY1:0:1:0:1:95:20",
          "DOMICILE:0:1:0:1:95:20",
          "MARKETCAP:0:1:0:1:95:20",
          "MKTCAPOVER50BLN:0:1:0:1:95:20"});
    SolrInputDocument doc4 = new SolrInputDocument();
    doc4.addField("id", 4);
    doc4.addField("ticker", new String[] { 
          "IBM:95:20:95:20:95:20",
          "FB:95:20:1:20:95:20",
          "ALLBBUNDCOMPANIES", // MALFORMED
          "COUNTRY1:0:1:0:1:95:20",
          "DOMICILE:0:1:0:1:95:20",
          "MARKETCAP:0:1:0:1:95:20",
          "MKTCAPOVER50BLN:0:1:0:1:95:20"});
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
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
  public void companyPerDocument() throws Exception {
    SolrQuery query = getQuery("id:1","test_companies_per_document_feature",1);
    String response = restTestHarness.query("/query?" + query.toString());
    float companies = getFeatureValue(response, "companies");
    assertEquals(1,companies,0.000001);
    
    query = getQuery("id:2","test_companies_per_document_feature",1);
    response = restTestHarness.query("/query?" + query.toString());
    companies = getFeatureValue(response, "companies");
    assertEquals(0,companies,0.000001);
    
    query = getQuery("id:3","test_companies_per_document_feature",1);
    response = restTestHarness.query("/query?" + query.toString());
    companies = getFeatureValue(response, "companies");
    assertEquals(2,companies,0.000001);
    
    query = getQuery("id:4","test_companies_per_document_feature",1);
    response = restTestHarness.query("/query?" + query.toString());
    companies = getFeatureValue(response, "companies");
    assertEquals(2,companies,0.000001);
    

  }  
  
}
