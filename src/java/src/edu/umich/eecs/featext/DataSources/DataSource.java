package edu.umich.eecs.featext.DataSources;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.Policies.ItemCache;

public interface DataSource {
	HashMap<Integer, Float> getFeaturesForDoc(int docId);
	HashMap<Integer, Float> getFeaturesForBlock(int docId);
	HashMap<Integer, Float> getDocsForFeature(int featureId);
	HashMap<Integer, Float> getBlocksForFeature(int featureId);
	String getFeatureName(int featureId);
	List<Integer> getBlockIds();
	List<Integer> getPageIds();
	String getDescription();
	int getId(String title);
	DataBlock getBlockContaining(int docId);
	DataBlock getBlockForItem(int itemId);
	DataBlock getBlockForOffset(long offset, long length);
	int getNewFeatureId();
	void addFeatureToDoc(int itemId, int newId);
	Set<Integer> getIdsForTest(int startBlock);
	Labeler getLabeler();
	List<Item> getItems(int n);
	List<Item> getTestItems(int numPerClass, List<String> classNames);
	List<Item> getStoppingItems(int numPerClass, List<String> classNames);
	ItemCache getItemCache();
	String getDocumentContents(int docId);
	void loadItem(Item item);
}
