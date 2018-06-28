package edu.umich.eecs.featext.DataSources;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

public class DemoDataSource implements DataSource {
	public static final String ID_STRING = "wikipedia";

	static Path fname;

	static DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex("wiki500DocIdx"); //("wikiW2VDocIdx");
	static FeatureDocumentsIndex featDocsIdx = new FeatureDocumentsIndex("wiki500InvIdx"); //("wikiW2VInvertedIdx");
	static FeatureNameDict featureIdToName = new FeatureNameDict("wiki500FeatureNameDict");

	private HashMap<Integer, HashMap<Integer, Float>> featureCache = new HashMap<Integer, HashMap<Integer, Float>>();

	private Labeler labeler = new DemoWikiLabeler();
	private HashMap<String, Integer> labelIds = new HashMap<String, Integer>();
	private HashMap<Integer, ArrayList<Integer>> labelDocs = new HashMap<Integer, ArrayList<Integer>>();

	private ArrayList<Integer> newIds = new ArrayList<Integer>();
	private HashMap<Integer, ArrayList<Integer>> newFeatureDocs = new HashMap<Integer, ArrayList<Integer>>();
	private HashMap<Integer, ArrayList<Integer>> newDocFeatures = new HashMap<Integer, ArrayList<Integer>>();

	private ItemCache itemCache = new ItemCache();

	Random rand = new Random();

	private HashSet<Integer> testSet;

	private HashMap<String, Integer> newFeatNames = new HashMap<String, Integer>();

	private long START_POSITION = 128;
	private long GIGABYTE = 1073741824;

	ArrayList<Path> fnameList = new ArrayList<Path>();

	public DemoDataSource(String fileName) {
		fname = new Path(fileName);
		fnameList.add(fname);

		for (int i = 1; i <= 2; i++) {
			Path fname2 = new Path(fileName.replace("0.seq", i + ".seq"));
			fnameList.add(fname2);
		}
		rand.setSeed(93829);
	}

	@Override
	public HashMap<Integer, Float> getFeaturesForDoc(int docId) {
		PostingsList pl;
		HashMap<Integer, Float> hm = featureCache.get(docId);

		Item item = itemCache.get(docId);

		if (hm == null) {
			try {
				pl = docIdx.get(docId);
				hm = pl.getPostings();

				// Add everything to arm 0
				hm.put(0, (float) 1.0);

				featureCache.put(docId, hm);
			} catch (ItemRetrievalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Add dynamically-created features
		if (newDocFeatures.containsKey(docId)) {
			for (Integer featId : newDocFeatures.get(docId)) {
				hm.put(featId, (float) 1.0);
			}
		}

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

		try {
			// if it's negative, we're using dynamic features
			if (featureId < 0) {
				for (Integer docId : newFeatureDocs.get(featureId)) {
					hm.put(docId, (float) 1.0);
				}
			} else {
				pl = featDocsIdx.get(featureId);
				hm = pl.getPostings();
			}
		} catch (ItemRetrievalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
		String name = featureIdToName.get(featureId);
		if (name == null) {
			name = "id["+featureId+"]";
		}
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



	//	public static void main(String[] args) {

	//	}

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


		// Read the test set IDs from a file if it exists
		File f = new File("test_data/logRegTestSetIds.txt");
		if(f.exists()) { 
			FileInputStream fstream;
			try {
				fstream = new FileInputStream(f);
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
				String strLine;
				//Read File Line By Line
				ArrayList<Integer> idsFromFile = new ArrayList<Integer>();
				while ((strLine = br.readLine()) != null)   {
					int id = Integer.parseInt(strLine.trim());
					testSet.add(id);
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
		else {
			int numPerClass = 1000;
			HashMap<String, Integer> classCounts = new HashMap<String, Integer>();

			for (String lbl : labeler.getLabels()) {
				classCounts.put(lbl, 0);
			}

			List<Integer> pageIds = WikiIndexer.getPageIds();

			Collections.shuffle(pageIds);
			for (int id : pageIds) {
				try {
					String cls = labeler.getLabel(id);

					if (classCounts.containsKey(cls) && classCounts.get(cls) < numPerClass) {
						classCounts.put(cls, classCounts.get(cls)+1);
						testSet.add(id);
						System.out.println(cls + " " + classCounts.get(cls));
					}	

					if (testSet.size() == numPerClass * labeler.getLabels().length) break;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}

			// Now write them to a file
			try {
				PrintWriter writer = new PrintWriter(f);
				for (int id : testSet) {
					writer.println(id);
				}
				writer.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	@Override
	public List<Item> getItems(int n) {
		List<Item> itemList = new ArrayList<Item>();

		//		Map<String, Integer> minorityCounts = new HashMap<String, Integer>();
		//		String[] labels = labeler.getLabels();
		//		for (String lbl : labels) {
		//			if (!lbl.equals("other")) {
		//				minorityCounts.put(lbl, 0);
		//			}
		//		}
int dupe = 0;
		int itemSortId = 1;
		try {
			for (Path filePath : fnameList) {
				DataBlock block = (DataBlock) new FSDataBlock(filePath, START_POSITION, GIGABYTE/50);
				IntWritable key = new IntWritable();
				Writable contents = new Text();

				long pos = block.getPosition();
				long oldPos = -1;

				while (block.next(key, contents) && itemList.size() < n) {
					long time1 = System.nanoTime();

					if (itemList.size() >= n) break;

					int itemId = key.get();

					oldPos = pos;
					pos = block.getPosition();
					long offset = oldPos;
					long length = pos - oldPos;

					Item item = itemCache.get(itemId);

					if (!item.isInitialized()) {
						Writable itemContents = new Text((Text) contents);
						item.initialize(itemContents, offset, length);

						// Set the item's sort ID. This works because the items
						// are randomly sorted in the data file. We want a stable,
						// but random ordering to the files.
						item.setSortId(itemSortId);
						itemSortId++;
					}
					else {
						System.out.println("seen it : "+ itemId + " : :"+ filePath.getName());
						// We've seen this item. It's a duplicate for some reason.
						continue;
					}
					long time2 = System.nanoTime();

					itemList.add(item);
					item.setLoadTime(time2 - time1);
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//Collections.shuffle(itemList, rand);
		return itemList;
	}

	@Override
	public List<Item> getTestItems(int numPerClass, List<String> classNames) {
		List<Item> testItems = new ArrayList<Item>();

		// Construct the items
		for (Integer itemId : testSet) {

			try {

				Item item = itemCache.get(itemId);

				if (!item.isInitialized()) {
					WikiPage wp = new WikiPage(itemId);
					Writable itemContents = new Text(wp.getText());
					item.initialize(itemContents, 0, 0);
				}
				else {
					// We've seen this item. It's a duplicate for some reason.
					continue;
				}

				testItems.add(item);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}

		return testItems;
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

	@Override
	public void loadItem(Item item) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Item> getStoppingItems(int numPerClass, List<String> classNames) {
		// TODO Auto-generated method stub
		return null;
	}

}
