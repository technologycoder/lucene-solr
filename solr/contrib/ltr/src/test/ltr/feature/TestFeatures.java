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
		// clean the current index before adding the documents
		assertU(delQ("*:*"));
		assertU(adoc("title", "bloomberg different bla", "description",
				"bloomberg", "id", "6", "popularity", "1", "content","blaTOP1 feature-BLA-test"));
		assertU(adoc("title", "bloomberg bloomberg ", "description",
				"bloomberg", "id", "7", "popularity", "2", "content", "blaTOP2"));
		assertU(adoc("title", "bloomberg bloomberg bloomberg", "description",
				"bloomberg", "id", "8", "popularity", "3"));
		assertU(adoc("title", "bloomberg bloomberg bloomberg bloomberg", "id",
				"9", "popularity", "5"));
		assertU(commit());
	}

	@AfterClass
	public static void after() throws Exception {
		aftertest();
	}

	private SolrQuery getQuery(String query, String featureModel, int rows) {
		SolrQuery q = new SolrQuery();
		q.setQuery(query);
		q.add("rows", String.valueOf(rows));
		// get the feature vector binding it to the field name fv
		q.add("fl", "*,score,fv:[fv model=" + featureModel + "]");
		q.add("wt", "json");
		return q;
	}

	@Test
	public void testQueryTypeFeature() throws Exception {
		SolrQuery query = getQuery("TOPIC.test TOPIC.test1 OR *:*",
				"test_query_type_feature", 1);

		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='query_has_topic:2.0'");
	}

	@Test
	public void testQueryScore() throws Exception {
		SolrQuery query = getQuery("title:bloomberg",
				"test_original_score_feature", 4);

		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='original_score:0.7768564'");
		assertJQ("/query?" + query.toString(),
				"/response/docs/[3]/fv=='original_score:0.3884282'");
	}

	@Test
	public void testQueryFeatures() throws Exception {
		SolrQuery query = getQuery("title:bloomberg", "query_features", 4);

		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='query_has_topic:0.0;original_score:0.7768564'");
	}

	@Test
	public void testFieldLengthFeature() throws Exception {
		SolrQuery query = getQuery("*:*", "test_field_length_feature", 4);
		// please note that title_length here should be 3.0, while in test we check
		// for 4.0. The reason is that solr stores the lengths of the fields in a 
		// compressed lossy format (1 byte per number), so sometimes the returned 
		// value could be slightly different. 
		// 
		// see also: https://lucene.apache.org/core/5_2_0/core/org/apache/lucene/codecs/lucene50/Lucene50NormsFormat.html
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='title_length:4.0;description_length:1.0'");
	}
	
	@Test
	public void testFieldLengthFeatureWhereAFieldIsMissing() throws Exception {
		SolrQuery query = getQuery("id:8 OR id:9", "test_content_field_length_feature", 4);
		
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='content_length:0.0'");
		assertJQ("/query?" + query.toString(),
				"/response/docs/[1]/fv=='content_length:0.0'");
	}
	
	
	

	@Test
	public void testFieldCharLengthFeature() throws Exception {
		SolrQuery query = getQuery("title:\"bloomberg different bla\"",
				"test_field_char_length_feature", 4);

		float titleCharLength = "bloomberg different bla".length();
		float descriptionCharLength = "bloomberg".length();

		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='title_char_length:" + titleCharLength
						+ ";description_char_length:" + descriptionCharLength
						+ "'");
	}
	
	@Test
	public void testFieldCharLengthFeatureWhereAFieldIsMissing() throws Exception {
		SolrQuery query = getQuery("id:8 OR id:9", "test_content_field_char_length_feature", 4);
		
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='content_char_length:0.0'");
		assertJQ("/query?" + query.toString(),
				"/response/docs/[1]/fv=='content_char_length:0.0'");
	}

	@Test
	public void testFunctionQueryFeature() throws Exception {
		SolrQuery query = getQuery("popularity:3",
				"test_function_query_feature", 4);

		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='powpolarity:9.0'");

		query.setQuery("popularity:5");

		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='powpolarity:25.0'");
	}

	@Test
	public void testConstantFeature() throws Exception {
		SolrQuery query = getQuery("*:*", "test_constant_feature", 1);

		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='c1:42.0'");
	}

	@Test
	public void testCompanionCodeFeatureIfQueryIsNotATerm() throws Exception {
		float defaultValue = 0;
		SolrQuery query = getQuery("*:*", "test_companioncode_feature", 1);
	
		
		// if the query is not not a term query, the feature return a default value
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='companion:"+defaultValue+";companion2:"+defaultValue+"'");
		
		query.setQuery("title:bla title:bloomberg");
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='companion:"+defaultValue+";companion2:"+defaultValue+"'");
		
	}
	
	@Test
	public void testCompanionCodeFeature() throws Exception {
		
		SolrQuery query = getQuery("title:bla", "test_companioncode_feature", 1);
		
		// if the query is not not a term query, the feature return a default value
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='companion:"+3.0+";companion2:"+42.0+"'");
	}
	
	@Test
	public void testCompanionCodeFeatureTestNoMatch() throws Exception {
		//test a matching companion code on a document that does not match
		SolrQuery query = getQuery("id:8", "test_companioncode_feature", 4);
		
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='companion:"+0f+";companion2:"+0f+"'");
	}
	
	@Test
	public void testCompanionCodeFeatureTestNoMatch2() throws Exception {
		// test a companion code on a document that matches the query but not 
		// the companion codes
		SolrQuery query = getQuery("title:bla", "test_companioncode_not_matching", 4);
		
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='companion:"+0f+"'");
	}
	
	@Test
	public void testCompanionCodeFeatureTestNoMatchSuppliedDefault() throws Exception {
		// test a companion code on a document that matches the query but not 
		// the companion codes
		SolrQuery query = getQuery("title:bla", "test_companioncode_not_matching_default_set_to_42", 4);
		
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='companion:"+42f+"'");
	}
	
	@Test
	public void testCompanionCodeFeatureMultipleMatches() throws Exception {
		// test multiple matches
		SolrQuery query = getQuery("title:bla", "test_companioncode_feature_multiple_matches", 1);
		
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='companion:111.0'");
	}
	
	// used to check the output of assertJQ
	private void print(String query, String nil) throws Exception{
		System.out.println("query: "+query);
		System.out.println(restTestHarness.query(query));
	}
	
	

	@Test
	public void testHasFieldFeature() throws Exception {
		// this document doesn't have the field description
		SolrQuery query = getQuery("id:9", "test_has_field_feature", 4);

		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='has_title:1.0;has_description:0.0'");

		query.setQuery("popularity:1");
		assertJQ("/query?" + query.toString(),
				"/response/docs/[0]/fv=='has_title:1.0;has_description:1.0'");
	}

}
