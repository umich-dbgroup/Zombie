package edu.umich.eecs.featext.DataSources;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.Policies.ItemCache;
import edu.umich.eecs.featext.Policies.QueueArm;
import edu.umich.eecs.featext.harness.ExperimentParameters;
import edu.umich.eecs.featext.index.DocumentFeaturesIndex;
import edu.umich.eecs.featext.index.FeatureDocumentsIndex;
import edu.umich.eecs.featext.index.FeatureNameDict;
import edu.umich.eecs.featext.index.FileOffset;
import edu.umich.eecs.featext.index.FileOffsetDict;
import edu.umich.eecs.featext.index.ItemRetrievalException;
import edu.umich.eecs.featext.index.PostingsList;

public class SynthRegressionDataSource implements DataSource {
	public static final String ID_STRING = "wikipedia";

	private HashMap<Integer, HashMap<Integer, Float>> featureCache = new HashMap<Integer, HashMap<Integer, Float>>();

	private WikiRegressionLabeler labeler = new WikiRegressionLabeler();
	private HashMap<String, Integer> labelIds = new HashMap<String, Integer>();
	private HashMap<Integer, ArrayList<Integer>> labelDocs = new HashMap<Integer, ArrayList<Integer>>();

	private ArrayList<Integer> newIds = new ArrayList<Integer>();
	private HashMap<Integer, ArrayList<Integer>> newFeatureDocs = new HashMap<Integer, ArrayList<Integer>>();
	private HashMap<Integer, ArrayList<Integer>> newDocFeatures = new HashMap<Integer, ArrayList<Integer>>();


	private ItemCache itemCache = new ItemCache();

	Random rand = new Random();

	static public long blockSize = 5242880; // bytes

	int startBlock = 42; // arbitrary number.
	double rarity = 0.0;

	private HashSet<Integer> testSet;
	private HashSet<Integer> stoppingSet;

	private HashMap<String, Integer> newFeatNames = new HashMap<String, Integer>();

	public SynthRegressionDataSource() {
		int testRunNum = 1;
		if (ExperimentParameters.get("testRunNumber") != null)
			testRunNum = Integer.valueOf(ExperimentParameters.get("testRunNumber"));

		startBlock += (testRunNum - 1) * 50;
		rand.setSeed(93829);

		if (ExperimentParameters.get("rarity") != null) {
			rarity = Double.valueOf(ExperimentParameters.get("rarity"));
		}


		buildTestSet();
		buildStoppingSet();

		// Load a random item to get some of the initial loading overhead
		// out of the way. This is hacky, but helps smooth out timing.

		try {
			WikiPage wp = new WikiPage(13900837);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	@Override
	public HashMap<Integer, Float> getFeaturesForDoc(int docId) {
		PostingsList pl;
		HashMap<Integer, Float> hm = featureCache.get(docId);

		Item item = itemCache.get(docId);

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
		String name = "id["+featureId+"]";

		return name;
	}

	@Override
	public List<Integer> getBlockIds() {
		ArrayList<Integer> ids = new ArrayList<Integer>();
		for (int i = 1; i <= 1764; i++) {
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
	public DataBlock getBlockContaining(int blockId) {

		DataBlock block = null;


		return block;
	}

	@Override
	public DataBlock getBlockForItem(int itemId) {

		DataBlock block = null;


		return block;
	}

	@Override
	public DataBlock getBlockForOffset(long offset, long length) {
		DataBlock block = null;

		return block;
	}


	@Override
	public int getId(String title) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override

	// This allows us to create new features dynamically and assign them to 
	// documents. Take care to do this serially.
	public int getNewFeatureId() {
		// We will use negative numbers for new ids. Just get the next 
		// negative number (decreasing)
		int newId = -(newIds.size() + 1);
		newIds.add(newId);
		return newId;
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

	public Set<Integer> getIdsForTest(int startBlock) {
		Set<Integer> ids = new HashSet<Integer>();

		// Open the file
		FileInputStream fstream;
		try {
			fstream = new FileInputStream("block_" + startBlock + ".txt");

			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

			String strLine;

			//Read File Line By Line
			while ((strLine = br.readLine()) != null)   {
				ids.add(Integer.parseInt(strLine.trim()));
			}

			//Close the input stream
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ids;
	}

	public Labeler getLabeler() {
		return labeler;
	}

	private void buildTestSet() {
		testSet = new HashSet<Integer>();

		List<Item> testItems = getTestItems(2000, new ArrayList<String>());

		Collections.shuffle(testItems);

		for (Item item : testItems) {
			testSet.add(item.getItemId());
		}
	}

	private void buildStoppingSet() {
		stoppingSet = new HashSet<Integer>();
	}

	public List<Integer> loadDocIds() throws IOException {
		String binFile = "test_data/wikipedia/page_ids.bin";
		List<Integer> list = new ArrayList<Integer>();
		DataInputStream bin = new DataInputStream(new BufferedInputStream(new FileInputStream(binFile)));
		while (true) {
			try {
				list.add(bin.readInt());
			} catch(EOFException eof) {
				break;
			}
		}
		bin.close();
		return list;
	}

	public void loadItem(Item item) {
		WikiPage wp = null;
		try {
			wp = new WikiPage(item.getItemId());
		} catch (Exception e) {
			e.printStackTrace();
		} 
		String contents = wp.getText();
		item.initialize(contents, 0, contents.length());
	}

	public double labelFunction(double x, double noiseLevel) {
		return (11.7 * x + 0 + noiseLevel * rand.nextGaussian()); 
	}

	@Override
	public List<Item> getItems(int n) {
		int numArms = 500;
		int goodArms = Integer.valueOf(ExperimentParameters.get("goodArmCount"));
		double maxX = 100;

		List<Item> itemList = new ArrayList<Item>();

		int itemSortId = 0;

		for (int i = 0; i < n; i++) {
			int itemId = i+1; // skip 0

			Item item = itemCache.get(itemId);

			int armId = (itemId % numArms)+1;

			// Alternatively, we could adjust the noise level based on armId. 
			// I.e., armId=1 -> low noise, armId=500 -> high noise, rest scaled in between
			
			boolean isGoodItem = armId <= goodArms;
			double featVal = rand.nextDouble() * maxX;
			//double label = (isGoodItem) ? labelFunction(featVal, 1.0) : labelFunction(featVal, 100);

			double label = labelFunction(featVal, 100 * (double) 4*armId*armId/(numArms*numArms));
			
			HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
			feature.put(armId, (float) featVal);
			featureCache.put(itemId, feature);

			item.setLabel(String.valueOf(label));
			item.initialize(String.valueOf(featVal), 0, 1);
			item.setSortId(itemSortId);
			itemSortId++;	

			itemList.add(item);
		}

		Collections.shuffle(itemList, rand);
		return itemList;
	}



	@Override
	public List<Item> getStoppingItems(int numPerClass, List<String> classNames) {
		List<Item> stoppingItems = new ArrayList<Item>();

		// Construct the items
		for (Integer itemId : stoppingSet) {
			try {
				Item item = itemCache.get(itemId);
				stoppingItems.add(item);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}

		return stoppingItems;
	}

	@Override
	public List<Item> getTestItems(int numPerClass, List<String> classNames) {
		int numArms = 500;
		int goodArms = 10;
		double maxX = 100;

		List<Item> itemList = new ArrayList<Item>();

		int itemSortId = 0;

		for (int i = 0; i < numPerClass; i++) {
			int itemId = 2000000+i; // skip 0

			Item item = itemCache.get(itemId);

			int armId = (itemId % numArms)+1;

			boolean isGoodItem = armId <= goodArms;
			double featVal = rand.nextDouble() * maxX;
			double label = labelFunction(featVal, 0.1);

			HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
			feature.put(armId, (float) featVal);
			featureCache.put(itemId, feature);

			item.setLabel(String.valueOf(label));
			item.initialize(String.valueOf(featVal), 0, 1);

			item.setSortId(itemSortId);
			itemSortId++;	

			itemList.add(item);
		}

		Collections.shuffle(itemList, rand);
		return itemList;
	}

	@Override
	public ItemCache getItemCache() {
		return itemCache;
	}

	@Override
	public String getDocumentContents(int docId) {
		String contents = null;
		try {
			WikiPage wp;
			wp = new WikiPage(docId);
			contents = wp.getText();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return contents;
	}

	private void loadClusterFeatures() {
		try {
			String clusterFile = "test_data/wikipedia/" + ExperimentParameters.get("clusterFile", "none.txt");
			BufferedReader br = new BufferedReader(new FileReader(clusterFile));
			String line;
			System.out.println("Loading features from: " + clusterFile);
			while ((line = br.readLine()) != null) {
				String[] elements = line.trim().split("\t");
				int docId = Integer.valueOf(elements[0]);
				int clusterId = Integer.valueOf(elements[1]);
				HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
				feature.put(clusterId, (float) 1.0);
				featureCache.put(docId, feature);
			}
			br.close();
		} catch (Exception e) {
			throw new RuntimeException("Error reading cluster file: " + e.getMessage());
		}
	}

}
