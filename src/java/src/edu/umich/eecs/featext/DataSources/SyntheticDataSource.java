package edu.umich.eecs.featext.DataSources;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.Text;

import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.Policies.ItemCache;
import edu.umich.eecs.featext.index.PostingsList;

public class SyntheticDataSource implements DataSource {
	public static final String ID_STRING = "synth";

	public String questionFile = "test_data/countries/questions.txt";
	
	public static final String docDir = "test_data/synth";

	private HashMap<String, Integer> featureMap = new HashMap<String, Integer>();

	private HashMap<Integer, HashMap<Integer, Float>> featureCache = new HashMap<Integer, HashMap<Integer, Float>>();

	
	private HashMap<String, Integer> titleToId = new HashMap<String, Integer>();

	private HashMap<Integer, String> featureNames = new HashMap<Integer, String>();
	
	private ArrayList<Integer> newIds = new ArrayList<Integer>();
	private HashMap<Integer, ArrayList<Integer>> newFeatureDocs = new HashMap<Integer, ArrayList<Integer>>();
	private HashMap<Integer, ArrayList<Integer>> newDocFeatures = new HashMap<Integer, ArrayList<Integer>>();

	private HashMap<String, Integer> newFeaturesNamesToIds = new HashMap<String, Integer>();
	
	private ItemCache itemCache = new ItemCache();

	private ArrayList<String> documents = new ArrayList<String>();
	private ArrayList<String> testDocuments = new ArrayList<String>();
	
	private Labeler labeler;
	
	int testPerClass = 1000;
	
	int totalClasses = 5;
	int taskClasses = 2;
	
	private Random rand = new Random();

	public SyntheticDataSource(int num, int classCount, double minorityFreq) {
		try {
			int minNum = (int) (num * minorityFreq);
			System.out.println("Num minority: " + minNum);
			
			labeler = new SyntheticLabeler(this, classCount);
			
			taskClasses = classCount;
			
			int[] numClasses = new int[taskClasses];
			for (int i = 0; i < taskClasses - 1; i++) {
				numClasses[i] = minNum;
			}
			// "other", the majority class, drawn equally from the remaining
			// class data files.
	

			numClasses[taskClasses - 1] = (num - (taskClasses - 1) * minNum);
			
			System.out.println(Arrays.toString(numClasses));
			loadDocs(numClasses);
			
			//rand.setSeed(252039475);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public HashMap<Integer, Float> getFeaturesForDoc(int docId) {
		PostingsList pl;
		HashMap<Integer, Float> hm = featureCache.get(docId);
		
		return hm;
	}

	@Override
	public HashMap<Integer, Float> getFeaturesForBlock(int blockId) {
		PostingsList pl;
		HashMap<Integer, Float> hm = null;

		return hm;
	}

	@Override
	public HashMap<Integer, Float> getDocsForFeature(int featureId) {
		PostingsList pl;
		HashMap<Integer, Float> hm = null;

		return hm;
	}

	@Override
	public HashMap<Integer, Float> getBlocksForFeature(int featureId) {
		PostingsList pl;
		HashMap<Integer, Float> hm = null;

		return hm;
	}

	@Override
	public String getFeatureName(int featureId) {
		return featureNames.get(featureId);
	}

	@Override
	public List<Integer> getBlockIds() {
		ArrayList<Integer> ids = new ArrayList<Integer>();
		for (int i = 1; i <= 1000; i++) {
			ids.add(i);
		}
		return ids;
	}

	@Override
	public List<Integer> getPageIds() {
		return WikiIndexer.getPageIds();
	}

	@Override
	public String getDescription() {
		return ID_STRING;
	}

	@Override
	// TODO: this always returns the same thing, no matter the blockId
	public DataBlock getBlockContaining(int blockId) {
		DataBlock block = new ArrayDataBlock(documents);
		return block;
	}

	public Item getItem(int itemId) {
		Item item = itemCache.get(itemId);

		if (!item.isInitialized()) {
			if (itemId <= documents.size()) {
				String contents = documents.get(itemId-1);
				item.initialize(new Text(contents), 0, 0);
			} else {
				String contents = testDocuments.get(itemId - documents.size() - 1);
				item.initialize(new Text(contents), 0, 0);
			}
		}

		return item;
	}

	public Item getTestItem(int testId) {
		Item item = itemCache.get(testId);

		int indexId = testId - documents.size() - 1;
		if (!item.isInitialized()) {
			String contents = testDocuments.get(indexId);
			item.initialize(new Text(contents), 0, 0);
		}

		return item;	
	}
	
	@Override
	public DataBlock getBlockForItem(int itemId) {
		DataBlock block = (DataBlock) new ItemDataBlock(getItem(itemId));
		return block;
	}

	@Override
	public DataBlock getBlockForOffset(long offset, long length) {
		DataBlock block = null;

		return block;
	}


	private void loadDocs(int[] numClasses) throws IOException {
		int[] actualNumClasses = new int[totalClasses];
		int[] numTestItems = new int[totalClasses];
		
		int minorityCount = taskClasses - 1;
		
		int remainingClasses = totalClasses - minorityCount;
		
		int otherNum = numClasses[numClasses.length-1]/remainingClasses;
		int lastNum = numClasses[numClasses.length-1] - (otherNum * (remainingClasses - 1));
		
		int otherTests = testPerClass/remainingClasses;
		int lastTests = testPerClass - (otherTests * (remainingClasses - 1));
		
		for (int i = 0; i < totalClasses; i++) {
		
			// OtherClass is an equal mixture of the remaining classes that 
			// were not specified. We need to adjust the totals to be equally
			// drawn from the rest.
			boolean isOtherClass = false;
			if (i >= taskClasses - 1) isOtherClass = true; 
			
			if (!isOtherClass) {
				actualNumClasses[i] = numClasses[i];
				numTestItems[i] = testPerClass;				
			}
			else {
		
				for (int j = i; j < totalClasses-1; j++) {
					actualNumClasses[j] = otherNum;
					numTestItems[j] = otherTests;
				}
				
				// Do the last one separately to get the remainders right.
				actualNumClasses[totalClasses-1] = lastNum;
				numTestItems[totalClasses-1] = lastTests;
			}
		}
		
		System.out.println(Arrays.toString(actualNumClasses));
		
	// Open the file
		for (int i = 0; i < totalClasses; i++) {
			int numToLoad = actualNumClasses[i];
			
			String dataFileName = docDir + "/c" + i + ".txt";
			int numRecords = countLines(dataFileName);
			System.out.println("Loading from " + dataFileName);
			
			
			FileInputStream fstream = new FileInputStream(dataFileName);
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream, "UTF8"));
			String strLine;
	
			// Read File Line By Line
			// We get the documents from the front of the file and the 
			// test documents from the end. 
			int n = 0;
			ArrayList<String> classTestDocs = new ArrayList<String>();
			while ((strLine = br.readLine()) != null)   {
				// Print the content on the console
				String doc = strLine.trim();

				if (n < numToLoad) {
					int docId = documents.size()+1;
					documents.add(doc);

					HashMap<Integer, Float> features = new HashMap<Integer, Float>();
					features.put(0, (float) 1.0); // global arm
					featureNames.put(0, "GLOBAL");
					
					// Skip the first token in the line; it's the class name
					boolean seenFirst = false;
					for (String tok : doc.split(" ")) {
						if (!seenFirst) {
							seenFirst = true;
							continue;
						}
						Integer tokId = featureMap.get(tok);
						if (tokId == null) {
							tokId = featureMap.size() + 1; // plus 1 to avoid 0
							featureMap.put(tok, tokId);
							featureNames.put(tokId, tok);
						}
						Float cnt = features.get(tokId);
						if (cnt == null) 
							features.put(tokId, (float) 1.0);
						else
							features.put(tokId, (float) (cnt + 1.0));
					}

					featureCache.put(docId, features);
				}
				else if (n >= numRecords - numTestItems[i]) {
					classTestDocs.add(doc);
				}
				n++;
			}
			//Close the input stream
			br.close();
			testDocuments.addAll(classTestDocs);
		}
		System.out.println("Loaded " + documents.size() + " training docs, " + testDocuments.size() + " test docs.");
	}

	@Override
	public int getId(String title) {
		// TODO Auto-generated method stub
		return 0;
	}

	// This allows us to create new features dynamically and assign them to 
	// documents. Take care to do this serially.
	public int getNewFeatureId() {
		// We will use negative numbers for new ids. Just get the next 
		// negative number (decreasing)
		int newId = -(newIds.size() + 1);
		newIds.add(newId);
		return newId;
	}
	
	public Integer getNewFeatureId(String featureName) {
		Integer featId = newFeaturesNamesToIds.get(featureName);
		if (featId == null) {
			featId = getNewFeatureId();
			newFeaturesNamesToIds.put(featureName, featId);
		}
		return featId;
	}

	@Override
	public void addFeatureToDoc(int docId, int newFeatId) {
		//ArrayList<Integer> docIds = newFeatureDocs.get(newFeatId);
		ArrayList<Integer> featIds = newDocFeatures.get(docId);

//		if (docIds == null) {
//			docIds = new ArrayList<Integer>();
//			newFeatureDocs.put(newFeatId, docIds);
//		}
		
		if (featIds == null) {
			featIds = new ArrayList<Integer>();
			newDocFeatures.put(docId, featIds);
		}

		//docIds.add(docId);
		featIds.add(newFeatId);
	}


	@Override
	public Set<Integer> getIdsForTest(int startBlock) {
		// TODO Auto-generated method stub
		return null;
	}

	public ArrayList<String> getDocuments() {
		return documents;
	}

	@Override
	public Labeler getLabeler() {
		return labeler;
	}

	@Override
	public List<Item> getItems(int n) {
		List<Item> itemList = new ArrayList<Item>();
		
		for (int i = 1; i <= n; i++) {
			itemList.add(getItem(i));
		}
		Collections.shuffle(itemList, rand);
		return itemList;
	}

	@Override
	public List<Item> getTestItems(int numPerClass, List<String> classNames) {
		List<Item> items = new ArrayList<Item>();
		for (int i = documents.size() + 1; i < documents.size() + testDocuments.size() + 1; i++) {
			items.add(getTestItem(i));
		}
		return items;
	}

	public List<Integer> getTestIds() {
		List<Integer> testIds = new ArrayList<Integer>();
		for (int i = documents.size() + 1; i < documents.size() + testDocuments.size() + 1; i++) {
			testIds.add(i);
		}
		return testIds;
	}

	public int countLines(String filename) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(filename));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
	
	public ItemCache getItemCache() {
		return itemCache;
	}

	@Override
	public String getDocumentContents(int docId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void loadItem(Item item) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Item> getStoppingItems(int numPerClass, List<String> classNames) {
		// TODO Auto-generated method stub
		return null;
	}
	

//	public static void main(String[] args) {
//		SyntheticDataSource data = new SyntheticDataSource();	
//	}
	
}
