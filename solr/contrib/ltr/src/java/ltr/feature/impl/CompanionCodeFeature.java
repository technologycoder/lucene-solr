package ltr.feature.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.ranking.FeatureScorer;
import ltr.ranking.FeatureWeight;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.LtrException;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This feature will check for the presence of one or more companion codes in case the
 * query is single term. See [NSREX-11].
 * This feature will accept two params:
 *   1. code_values
 *   2. field
 *
 *   e.g.
 *       "params": {
 *            "code_values" : {
 *                "${q}TEST": 1.0,
 *                "feature-${q}-TEST": 42.0,
 *                "${q}TOP": 2.0,
 *                "${q}TOP1": 3.0
 *              },
 *            "field":"content",
 *            "default_value":10.0
 *        },
 *
 * code_values will contain the companion codes and the feature value mapped to each
 * companion code. ${q} will be replaced with the current query,
 * but it's not mandatory to specify a query, a companion code can be also a simple string.
 * 
 *
 * Each companion code will be matched against the field specified (content, in the example) and
 * if the field contains a string that matches the companion code, the assigned feature value
 * for the companion code will be returned. The matching follows the declaration order
 * so in case of multiple matches, the value of the feature will be the value
 * associated with the first companion code that matched within the field.
 * 
 * If the query is not a simple term, or if no companion code was matched in the document, this feature
 * will return the value declared in the default_value field.
 *
 * Example: the feature in the example applied on "content: topic.OILTOP1 topic.OILTEST  "
 * will produce the value 1.0 ,since ${q}TEST will be the first companion code matched on the
 * field.
 *
 *
 * TODO: we could think of having this value returning the average of the scores 
 * when applied to multi-term queries (up to a certain number of topics AND using 
 * only the "positive" ones -- i.e., not the ones like "NOT TOPIC:OIL").
 *
 */
public class CompanionCodeFeature extends Feature {

	private static final Logger logger = LoggerFactory
			.getLogger(CompanionCodeFeature.class);

	private static final String CODE_VALUES_PARAM = "code_values";
	private static final String DEFAULT_VALUE_PARAM = "default_value";

	// the default field where to look for the companion codes
	private static final String DEFAULT_FIELD = "content";

	private Double defaultFeatureValue = 0d;

	
	private String[] companionCodes;
	private float[] companionCodeFeatureValue;
	private String companionCodeField;

	public CompanionCodeFeature() {
	}

	@SuppressWarnings("unchecked")
	public void init(String name, NamedParams params, int id) throws FeatureException {
		super.init(name, params, id);
		companionCodes = new String[0];
		companionCodeFeatureValue = new float[0];
		Map<String, Double> companionToFeatureScore;

		if (params.containsKey(CODE_VALUES_PARAM))
		{
			companionToFeatureScore = (Map<String, Double>) params
					.get(CODE_VALUES_PARAM);
		} else {
			logger.error("parsing companion code feature {}, cannot find {}",name, CODE_VALUES_PARAM);
			throw new FeatureException("parsing companion code feature");
		}
		
		defaultFeatureValue = params.getDouble(DEFAULT_VALUE_PARAM, defaultFeatureValue);
		
		companionCodeField = params.getString(
				CommonLtrParams.FIELD, DEFAULT_FIELD);
		
		
		companionCodes = new String[companionToFeatureScore.size()];
		companionCodeFeatureValue = new float[companionToFeatureScore.size()];
		int index = 0;
		// reads the mapping companion code -> feature value
		for (Map.Entry<String, Double> companionCodeAndFeatureValue : companionToFeatureScore
				.entrySet()) {

			companionCodes[index] = companionCodeAndFeatureValue.getKey();
			// real are parsed as Double, so we need to convert them to floats
			// here.
			companionCodeFeatureValue[index++] = companionCodeAndFeatureValue
					.getValue().floatValue();
		}
	}

	@Override
	public FeatureWeight createWeight(IndexSearcher searcher)
			throws IOException {
		return new CompanionCodeWeight(searcher, name, params, norm, id);
	}

	@Override
	public String toString() {
		return "CompanionCodeFeature";
	}

	public class CompanionCodeWeight extends FeatureWeight {

		Set<Term> terms = new HashSet<>();
		// in order to match if a document contains the specified companion 
		// code, we extract the posting list for the term representing the companion
		// code, and we check if the docid is in the posting list. 
		// This array of weights will contain the weight of each TermQuery created
		// for each companion code. 
		Weight[] weights = new Weight[companionCodeFeatureValue.length];

		public CompanionCodeWeight(IndexSearcher searcher, String name,
				NamedParams params, Normalizer norm, int id) {
			super(searcher, name, params, norm, id);
		}

		@Override
		public Query getQuery() {
			return CompanionCodeFeature.this;
		}
		
		@Override
		public void setOriginalQuery(Query originalQuery) throws IOException {
			if (originalQuery == null) {
				// if the query is not present ignore
				logger.debug("original query is null");
				return;
			}
			// if the query is not a single term query ignore
			originalQuery.extractTerms(terms);
			if (terms.size() != 1) {
				logger.debug("original query contains more than one term ({})", terms);
				return;
			}
			// the query contains exactly one term
			Term term = terms.iterator().next();
			// Set the original query, since this method is called to prepare
			// the feature computation for all the documents, it will also 
			// take care to produce the weights for all the companion code 
			// queries. 
			// TODO add a prepare() method called before the feature values generation
			for (int index = 0; index < companionCodes.length; index++) {

				Query tq = null;
				String termText = term.text();
				// We need to extract the posting list for each companion code, 
				// so we build a query for each companion code. If the companion
				// code mentions the query, we replace the query before building
				// the query.
				String query = companionCodeField + ":"
						+ companionCodes[index].replace("${q}", termText);
				try {

					QParser parser = QParser
							.getParser(query, "lucene", request);
					tq = parser.parse();
				} catch (SyntaxError e) {
					logger.warn("error parsing companion code query {} ", query);
					continue;
				}
				weights[index] = tq.createWeight(searcher);
				weights[index].getValueForNormalization();
			}

		}

		@Override
		public FeatureScorer scorer(AtomicReaderContext context, Bits acceptDocs)
				throws IOException {

			if (terms.size() != 1) {
				// if the query was not a unique term, just return a default
				// value
				// FIXME getDefaultValue is a method in the scorer
				// so we cannot call it here.. We will probably have to 
				// move getdefaultValue in the weight.
				return new ConstantFeatureScorer(this, defaultFeatureValue.floatValue(),
						"CompanionCodeFeature");
			}
			Scorer[] companionCodeScorers = new Scorer[weights.length];
			for (int index = 0; index < weights.length; index++) {
				if (weights[index] != null) {
					Scorer scorer = weights[index].scorer(context,
							Weight.PostingFeatures.DOCS_AND_FREQS, acceptDocs);
					companionCodeScorers[index] = scorer;
				}
			}
			return new CompanionCodeScorer(this, context, companionCodeScorers);
		}

	    /*
	     * This scorer will wrap all the scores produced for the companion code
	     * term queries, and will return as score the value associated with the 
	     * first companion code matching in the list for the current document.
	     */
		public class CompanionCodeScorer extends FeatureScorer {
			
			private Scorer[] companionCodeScorers = null;

			public CompanionCodeScorer(FeatureWeight weight,
					AtomicReaderContext context, Scorer[] companionCodeScorers) {
				super(weight);
				this.companionCodeScorers = companionCodeScorers;
			}

			@Override
			public float score() throws IOException {
				if (terms.size() != 1) {
					return defaultFeatureValue.floatValue();
				}
				for (int index = 0; index < companionCodeScorers.length; index++) {
					if (companionCodeScorers[index] == null) {
						continue;
					}
					// iterates on the posting list of this companion code. If we
					// found the docId, it means that the companion code was
					// the given document, and we can return the feature value
					// associated with the companion code.
					if (companionCodeScorers[index].advance(docID) == docID) {
						return companionCodeFeatureValue[index];
					}
				}
				return defaultFeatureValue.floatValue();
			}

			@Override
			public String toString() {
				return "CompanionCodeFeature [" + Arrays.toString(companionCodes)
						+ "]";
			}

		}
	}

}
