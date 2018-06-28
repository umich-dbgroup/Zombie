package edu.umich.eecs.featext;

import java.util.Map;

import edu.umich.eecs.featext.index.DocumentFeaturesIndex;
import edu.umich.eecs.featext.index.FeatureDict;
import edu.umich.eecs.featext.index.FeatureDocumentsIndex;
import edu.umich.eecs.featext.index.FeatureNameDict;
import edu.umich.eecs.featext.index.ItemRetrievalException;
import edu.umich.eecs.featext.index.PostingsList;


public class UDFFeatures {
	public static Config conf = new Config();
	
	public static DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex("UDFDocIdx");
	public static FeatureDocumentsIndex invertIdx = new FeatureDocumentsIndex("UDFInvertedIdx");
	public static FeatureDict featureDict = new FeatureDict("UDFFeatureDict");	
	public static FeatureNameDict featureIdToName = new FeatureNameDict("UDFFeatureNameDict");
	
	public UDFFeatures() { }

	private static float scaleAmt = (float) 20.0;
	
	public static int getFeatureId(String dataSetName, String taskName, String udfName) throws ItemRetrievalException {
		String featureName = dataSetName + "|" + taskName + "|" + udfName;
		
		int featId = featureDict.get(featureName);
		if ((featId = featureDict.get(featureName)) == -1) {			
			featId = featureIdToName.getMaxKey() + 1;
			
			featureDict.put(featureName, featId);
			featureIdToName.put(featId, featureName);
			
			featureDict.syncDatabase();
			featureIdToName.syncDatabase();
		}
		
		return featId;
	}
	
	
	private static void addFeatureValue(int featureId, int docId, float value) throws ItemRetrievalException {
		if (value > 0) {
			PostingsList docPostings = docIdx.get(docId); // new PostingsList(docId);
			PostingsList invPostings = invertIdx.get(featureId);
				
			// Add the posting to the doc item and inverted index
			docPostings.addItem(featureId, scaleAmt * value);
			invPostings.addItem(docId, scaleAmt * value);

			docIdx.put(docPostings);
			invertIdx.put(invPostings);
	

		}
	}
	
	public static void indexFeatureSet(String dataSetName, String taskName, String udfName, Map<Integer, Double> itemVals) {
		try {
			int featureId = getFeatureId(dataSetName, taskName, udfName);
			//int negFeatureId = getFeatureId(dataSetName, taskName, "~not~"+udfName);

			for (Map.Entry<Integer, Double> itemVal : itemVals.entrySet()) {
					addFeatureValue(featureId, itemVal.getKey(), new Float(itemVal.getValue()));
					//addFeatureValue(negFeatureId, itemVal.getKey(), (float) (1.0 - itemVal.getValue()));
			}
			
			docIdx.syncDatabase();
			invertIdx.syncDatabase();
		} catch (ItemRetrievalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
