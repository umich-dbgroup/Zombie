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

public class WikipediaRegressionDataSource implements DataSource {
	public static final String ID_STRING = "wikipedia";

	static Path fname = new Path("test_data/wikipedia/wikiRandom.seq");
	static Path oldFname = new Path("test_data/wikipedia/wiki.seq");

	static DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex("wiki500DocIdx"); //("wikiW2VDocIdx");
	static FeatureDocumentsIndex featDocsIdx = new FeatureDocumentsIndex("wiki500InvIdx"); //("wikiW2VInvertedIdx");
	static FeatureNameDict featureIdToName = new FeatureNameDict("wiki500FeatureNameDict");

	static FileOffsetDict offsetDict = new FileOffsetDict("offsetDict");

	static FileOffsetDict blockOffsetDict = new FileOffsetDict("wikiRandomBlockOffsets");
	static DocumentFeaturesIndex blockFeaturesIdx = new DocumentFeaturesIndex("wikiRandomBlockFeatures");
	static FeatureDocumentsIndex featureBlocksIdx = new FeatureDocumentsIndex("wikiRandomFeatureBlocks");

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

	public WikipediaRegressionDataSource() {
		int testRunNum = 1;
		if (ExperimentParameters.get("testRunNumber") != null)
			testRunNum = Integer.valueOf(ExperimentParameters.get("testRunNumber"));

		startBlock += (testRunNum - 1) * 50;
		rand.setSeed(93829);

		if (ExperimentParameters.get("rarity") != null) {
			rarity = Double.valueOf(ExperimentParameters.get("rarity"));
		}

		if (ExperimentParameters.get("indexType") != null) {
			if (ExperimentParameters.get("indexType").equals("w2v")) {
				docIdx = new DocumentFeaturesIndex("wikiW2VDocIdx");
				featDocsIdx = new FeatureDocumentsIndex("wikiW2VInvertedIdx");
			}
			else if (ExperimentParameters.get("indexType").equals("cluster")) {
				loadClusterFeatures();
			}

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
		String sizeLabel = "_sm";
		if (item.size() > 10000) sizeLabel = "_lg";
		else if (item.size() > 5000) sizeLabel = "_md";

		if (hm == null) {
			if (ExperimentParameters.get("randomArmCount") != null) {
				int armCount = Integer.valueOf(ExperimentParameters.get("randomArmCount"));
				hm = new HashMap<Integer, Float>();
				hm.put(0,  (float) 1.0);
				int randArmId = rand.nextInt(armCount) + 1;
				hm.put(randArmId, (float) 1.0);
			}
			else {
				try {
					pl = docIdx.get(docId);
					hm = pl.getPostings();

					if (ExperimentParameters.get("splitArmsBySize") != null) {
						for (int id : hm.keySet()) {
							String newName = id + sizeLabel;
							Integer featId = newFeatNames.get(newName);
							if (featId == null) {
								featId = getNewFeatureId();
								newFeatNames.put(newName, featId);
							}
							addFeatureToDoc(docId, featId);
						}
					}

					// Add everything to arm 0
					hm.put(0, (float) 1.0);

					//featureCache.put(docId, hm);
				} catch (ItemRetrievalException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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

		try {
			pl = blockFeaturesIdx.get(blockId);
			hm = pl.getPostings();
		} catch (ItemRetrievalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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

		try {
			pl = featureBlocksIdx.get(featureId);
			hm = pl.getPostings();
		} catch (ItemRetrievalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
		FileOffset fo = blockOffsetDict.get(blockId);

		DataBlock block = null;
		try {
			block = (DataBlock) new FSDataBlock(fname, fo.getOffset(), (long) fo.getLength());
			block.setBlockId(blockId);
			//block = (DataBlock) new FSDataBlock(fname, fo.getOffset(), blockSize);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return block;
	}

	@Override
	public DataBlock getBlockForItem(int itemId) {
		FileOffset fo = offsetDict.get(itemId);

		DataBlock block = null;
		try {
			block = (DataBlock) new FSDataBlock(oldFname, fo.getOffset(), (long) fo.getLength());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return block;
	}

	@Override
	public DataBlock getBlockForOffset(long offset, long length) {
		DataBlock block = null;
		try {
			block = (DataBlock) new FSDataBlock(fname, offset, length);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return block;
	}

	public void reorderSequenceFile() throws IOException {
		SequenceFile.Reader reader = null;
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		Path oldFname = new Path("test_data/wikipedia/wiki.seq");

		reader = new SequenceFile.Reader(fs, oldFname, conf);

		Path newFname = new Path("test_data/wikipedia/wikiRandom.seq");

		IntWritable key = new IntWritable();
		Text textVal = new Text();
		SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, newFname, key.getClass(), textVal.getClass());

		try {
			key = (IntWritable) reader.getKeyClass().newInstance();

			Writable val = (Writable) reader.getValueClass().newInstance();

			int i = 0;
			ArrayList<Integer> ids = new ArrayList<Integer>();
			while (reader.next(key, val)) {
				System.out.println(++i + " Getting: " + key.get());
				ids.add(key.get());
			}
			//Collections.sort(ids);
			Collections.shuffle(ids);

			for (int id : ids) {
				System.out.println("Adding: " + id);
				WikiPage wp = new WikiPage(id);
				key.set(wp.getPageId());
				textVal.set(new Text(wp.getTitle()+"\n"+wp.getText()));   
				out.append(key, textVal);
			}


		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(out);
		}
	}

	public void splitSequenceFile() throws IOException {

		int numOutputFiles = 88; // will make about 100MB each

		SequenceFile.Reader reader = null;
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		Path oldFname = new Path("test_data/wikipedia/wikiRandom.seq");

		reader = new SequenceFile.Reader(fs, oldFname, conf);


		IntWritable key = new IntWritable();
		Text textVal = new Text();
		SequenceFile.Writer[] out = new SequenceFile.Writer[numOutputFiles];

		for (int i = 0; i < numOutputFiles; i++) {
			Path newFname = new Path("test_data/wikipedia/splits/" +String.format("%03d", i) + ".seq");
			out[i] = SequenceFile.createWriter(fs, conf, newFname, key.getClass(), textVal.getClass());
		}

		try {
			key = (IntWritable) reader.getKeyClass().newInstance();
			Writable val = (Writable) reader.getValueClass().newInstance();

			while (reader.next(key, val)) {
				int fileNum = rand.nextInt(numOutputFiles);
				out[fileNum].append(key, val);
			}

		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			for (int i = 0; i < numOutputFiles; i++) {
				IOUtils.closeStream(out[i]);
			}

		}
	}

	public void countFeatures() throws IOException {
		featDocsIdx.startTraversal();
		int maxId = 0;
		PostingsList p;

		// 92604
		File file = new File("featureCounts.txt");
		// if file doesn't exist, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);		

		while ((p = featDocsIdx.getNext()) != null) {
			int featId = p.getKey();
			HashMap<Integer, Float> postings = p.getPostings();
			String name = getFeatureName(featId);
			int count = postings.size();

			System.out.println(featId + "\t" + name + "\t" + count);
			bw.write(featId + "\t" + name + "\t" + count + "\n");
		}
		bw.close();

		featDocsIdx.endTraversal();	
	}

	public void dumpFeatures() throws IOException {
		featureBlocksIdx.startTraversal();
		int maxId = 0;
		PostingsList p;

		// 92604
		File file = new File("featuresDump.txt");
		// if file doesn't exist, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);		

		while ((p = featureBlocksIdx.getNext()) != null) {
			int featId = p.getKey();
			System.out.println(featId);
			int[] relatedFeatures = new int[96205];
			relatedFeatures[0] = featId;

			HashMap<Integer, Float> blocks = getBlocksForFeature(featId);
			for (Map.Entry<Integer, Float> posting : blocks.entrySet()) {
				int blockId = posting.getKey();
				HashMap<Integer, Float> features = getFeaturesForBlock(blockId);
				for (Map.Entry<Integer, Float> entry : features.entrySet()) {
					int fId = entry.getKey(); 
					int fVal = Math.round(entry.getValue()); // converts to int (hack?)
					relatedFeatures[fId] += fVal;
				}
			}
			System.out.println(" --- " + relatedFeatures.length);
			bw.write(Arrays.toString(relatedFeatures) + "\n");
		}
		bw.close();

		featureBlocksIdx.endTraversal();


		//		blockFeaturesIdx.startTraversal();
		//		PostingsList p = null;
		//
		//		File file = new File("featuresDump.txt");
		//
		//		// if file doesn't exist, then create it
		//		if (!file.exists()) {
		//			file.createNewFile();
		//		}
		//
		//		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		//		BufferedWriter bw = new BufferedWriter(fw);
		//
		//		int i = 0;
		//		while ((p = blockFeaturesIdx.getNext()) != null) {
		//			System.out.println(i);
		//			bw.write(p.getKey()+","+p.getPostings()+"\n");
		//		}
		//		bw.close();
		//
		//		blockFeaturesIdx.endTraversal();
	}

	//	public static void main(String[] args) {
	//		WikipediaDataSource data = new WikipediaDataSource();
	//
	//		int blockId = 1;
	//		Configuration conf = new Configuration();
	//		FileSystem fs = null;
	//		SequenceFile.Reader reader = null;
	//
	//		try {
	//			data.splitSequenceFile();
	//		} catch (IOException e) {
	//			// TODO Auto-generated catch block
	//			e.printStackTrace();
	//		}
	//
	//		//		try {
	//		//			data.countFeatures();
	//		//		} catch (IOException e) {
	//		//			// TODO Auto-generated catch block
	//		//			e.printStackTrace();
	//		//		}
	//
	//
	//		//		try {
	//		//			data.dumpFeatures();
	//		//		} catch (IOException e) {
	//		//			// TODO Auto-generated catch block
	//		//			e.printStackTrace();
	//		//		}
	//		//		
	//		//				try {
	//		//					data.reorderSequenceFile();
	//		//				} catch (IOException e) {
	//		//					// TODO Auto-generated catch block
	//		//					e.printStackTrace();
	//		//				} 
	//
	//		//				try {
	//		//					Path newFname = new Path("test_data/wikipedia/wikiRandom.seq");
	//		//		
	//		//					fs = FileSystem.get(conf);
	//		//					System.out.println(fname);
	//		//		
	//		//					reader = new SequenceFile.Reader(fs, newFname, conf);
	//		//		
	//		//					IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
	//		//					Writable val = (Writable) reader.getValueClass().newInstance();
	//		//		
	//		//					System.out.println(reader.getPosition());
	//		//					int size = 0;
	//		//					long startPos = reader.getPosition();
	//		//					long curPos = startPos;
	//		//					long lastPos = 0;
	//		//					int docCount = 0;
	//		//		
	//		//					HashMap<Integer, Float> blockFeatures = new HashMap<Integer, Float>();
	//		//		
	//		//					while (reader.next(key, val)) {
	//		//						curPos = reader.getPosition();
	//		//						size = (int) (curPos - startPos);
	//		//						docCount++;
	//		//						int docId = key.get();
	//		//						HashMap<Integer, Float> features = data.getFeaturesForDoc(docId);
	//		//		
	//		//						// Collect the features for each item into the block features aggregator
	//		//						for (Map.Entry<Integer, Float> entry : features.entrySet()) {
	//		//							Float value = blockFeatures.get(entry.getKey());
	//		//							if (value == null) {
	//		//								blockFeatures.put(entry.getKey(), entry.getValue());
	//		//							}
	//		//							else {
	//		//								blockFeatures.put(entry.getKey(), value + entry.getValue());
	//		//							}
	//		//						}
	//		//		
	//		//						// If we're over the size of the block, save it up to just before this item
	//		//						if (size > blockSize) {
	//		//							size = (int) (lastPos - startPos);
	//		//							System.out.println(blockId + " - " + startPos + ":" + (lastPos - 1) + " - " + size + ", Docs: " + docCount);
	//		//							FileOffset fo = new FileOffset(startPos, size);
	//		//							blockOffsetDict.put(blockId, fo);
	//		//		
	//		//							// Add the features to the block indexes
	//		//							PostingsList pl = new PostingsList(blockId, blockFeatures);
	//		//							blockFeaturesIdx.put(pl);
	//		//		
	//		//							// Now for the inverse index
	//		//							for (Map.Entry<Integer, Float> posting : blockFeatures.entrySet()) {
	//		//								int featId = posting.getKey();
	//		//								float value = posting.getValue();
	//		//		
	//		//								PostingsList featPost = new PostingsList(featId);
	//		//								featPost.addItem(blockId, value);	
	//		//								try {
	//		//									featureBlocksIdx.merge(featPost);
	//		//								} catch (ItemRetrievalException e) {
	//		//									// TODO Auto-generated catch block
	//		//									e.printStackTrace();
	//		//								}
	//		//							}
	//		//							featureBlocksIdx.syncDatabase();
	//		//							blockFeaturesIdx.syncDatabase();
	//		//							blockOffsetDict.syncDatabase();
	//		//		
	//		//							docCount = 0;
	//		//							blockId++;
	//		//							startPos = lastPos;
	//		//							blockFeatures = new HashMap<Integer, Float>();
	//		//		
	//		//						}
	//		//						lastPos = curPos;
	//		//					}
	//		//		
	//		//		
	//		//		
	//		//				} catch (IOException e) {
	//		//					// TODO Auto-generated catch block
	//		//					e.printStackTrace();
	//		//				} catch (InstantiationException e) {
	//		//					// TODO Auto-generated catch block
	//		//					e.printStackTrace();
	//		//				} catch (IllegalAccessException e) {
	//		//					// TODO Auto-generated catch block
	//		//					e.printStackTrace();
	//		//				}
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

		//String testIdFile = "test_data/linRegTestSetIds_videogames.txt";
		String testIdFile = "test_data/linRegTestSetIds.txt";

		System.out.println("Loading test IDs from " + testIdFile);

		File f = new File(testIdFile);

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
			List<Integer> pageIds;
			System.out.println("Loading ids");
			try {
				pageIds = loadDocIds();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Can't load page IDs.");
			}

			Collections.shuffle(pageIds);

			for (Integer id : pageIds) {
				if (Double.valueOf(labeler.getLabel(id)) > 0.0) {
					testSet.add(id);
					System.out.println("test set size: " + testSet.size());
				}
				if (testSet.size() >= 100) break;
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

	private void buildStoppingSet() {
		stoppingSet = new HashSet<Integer>();

		// Read the test set IDs from a file if it exists
		String testIdFile = "test_data/logRegStoppingSetIds.txt";
		if (ExperimentParameters.check("numClasses", "2"))
			testIdFile = "test_data/logRegStoppingSetIds.txt.2class";
		else if (ExperimentParameters.check("numClasses", "6"))
			testIdFile = "test_data/logRegStoppingSetIds.txt.6class";

		System.out.println("Loading test IDs from " + testIdFile);

		File f = new File(testIdFile);

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
					if (testSet.contains(id)) continue;
					stoppingSet.add(id);
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
			int totalNumber = 960;
			HashMap<String, Integer> classCounts = new HashMap<String, Integer>();

			for (String lbl : labeler.getLabels()) {
				classCounts.put(lbl, 0);
			}

			int numPerClass = totalNumber/classCounts.size();

			List<Integer> pageIds = WikiIndexer.getPageIds();

			Collections.shuffle(pageIds);
			for (int id : pageIds) {
				if (testSet.contains(id)) continue;

				try {
					String cls = labeler.getLabel(id);

					if (classCounts.containsKey(cls) && classCounts.get(cls) < numPerClass) {
						classCounts.put(cls, classCounts.get(cls)+1);
						stoppingSet.add(id);
						System.out.println(cls + " " + classCounts.get(cls));
					}	

					if (stoppingSet.size() == numPerClass * labeler.getLabels().length) break;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}

			// Now write them to a file
			try {
				PrintWriter writer = new PrintWriter(f);
				for (int id : stoppingSet) {
					writer.println(id);
				}
				writer.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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

	@Override
	public List<Item> getItems(int n) {
		Integer nextId = startBlock;
		List<Integer> pageIds;
		System.out.println("Loading ids");
		try {
			pageIds = loadDocIds();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("Can't load page IDs.");
		}

		List<Item> itemList = new ArrayList<Item>();

		Map<String, Integer> labelCounts = new HashMap<String, Integer>();
		String[] labels = labeler.getLabels();
		for (String lbl : labels) {
			labelCounts.put(lbl, 0);
		}

		int maxMinorityCount = (int) (n * rarity);
		int maxMajorityCount = n - (labels.length - 1) * maxMinorityCount;

		int itemSortId = 1;

		for (int itemId : pageIds) {
			if (itemList.size() >= n || itemList.size() >= pageIds.size()) break;

			// Make sure the items aren't included in the hold-out set
			if (testSet.contains(itemId) || stoppingSet.contains(itemId)) continue;

			// if we are using cluster-based features, make sure that we actually
			// clustered this item. We only clustered a subset of the entire corpus
			// since our method didn't scale to the whole thing.
			if (ExperimentParameters.check("indexType", "cluster")) {
				if (!featureCache.containsKey(itemId)) continue;
			}

			Item item = itemCache.get(itemId);
			if (rarity > 0) {
				String label = labeler.getLabel(itemId);
				if (labelCounts.get(label) != null) {
					if (!label.equals("other") && labelCounts.get(label) >= maxMinorityCount) continue;
					else if (label.equals("other") && labelCounts.get(label) >= maxMajorityCount) continue;

					labelCounts.put(label, labelCounts.get(label) + 1);
				}
				item.setLabel(label);
			}

			item.setSortId(itemSortId);
			itemSortId++;	

			itemList.add(item);
		}

		Collections.shuffle(itemList, rand);
		System.out.println("Labels: " + labelCounts);
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
		List<Item> testItems = new ArrayList<Item>();

		// Construct the items
		for (Integer itemId : testSet) {
			try {
				Item item = itemCache.get(itemId);
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
