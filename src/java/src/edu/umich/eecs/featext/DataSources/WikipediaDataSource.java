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

import org.apache.commons.math3.distribution.BetaDistribution;;

public class WikipediaDataSource implements DataSource {
	public static final String ID_STRING = "wikipedia";

	static Path fname = new Path("test_data/wikipedia/wikiRandom.seq");
	static Path oldFname = new Path("test_data/wikipedia/wiki.seq");

	static DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex("wiki500DocIdx"); //("wikiW2VDocIdx");
	static FeatureDocumentsIndex featDocsIdx = new FeatureDocumentsIndex("wiki500InvIdx"); //("wikiW2VInvertedIdx");
	static FeatureNameDict featureIdToName = new FeatureNameDict("wiki500FeatureNameDict");

	static FileOffsetDict offsetDict = new FileOffsetDict("offsetDict");

	//	static FileOffsetDict blockOffsetDict = new FileOffsetDict("wikiRandomBlockOffsets");
	//	static DocumentFeaturesIndex blockFeaturesIdx = new DocumentFeaturesIndex("wikiRandomBlockFeatures");
	static FeatureDocumentsIndex featureBlocksIdx = new FeatureDocumentsIndex("wikiRandomFeatureBlocks");

	private HashMap<Integer, HashMap<Integer, Float>> featureCache = new HashMap<Integer, HashMap<Integer, Float>>();

	private WikiLabeler labeler = new WikiLabeler();
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

	public WikipediaDataSource() {
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
			else if (ExperimentParameters.get("indexType").equals("lsh")) {
				loadLSHFeatures();
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
				//featureCache.put(docId, hm);

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

					//	featureCache.put(docId, hm);
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

		//		try {
		//			pl = blockFeaturesIdx.get(blockId);
		//			hm = pl.getPostings();
		//		} catch (ItemRetrievalException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}

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

		//		try {
		//			pl = featureBlocksIdx.get(featureId);
		//			hm = pl.getPostings();
		//		} catch (ItemRetrievalException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}

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
		//		FileOffset fo = blockOffsetDict.get(blockId);

		DataBlock block = null;
		//		try {
		//			block = (DataBlock) new FSDataBlock(fname, fo.getOffset(), (long) fo.getLength());
		//			block.setBlockId(blockId);
		//			//block = (DataBlock) new FSDataBlock(fname, fo.getOffset(), blockSize);
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}

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

	//	public void dumpFeatures() throws IOException {
	//		docIdx.startTraversal();
	//		int maxId = 0;
	//		PostingsList p;
	//
	//		System.out.println("here");
	//		
	//		// 92604
	//		File file = new File("featureDocCounts.txt");
	//		// if file doesn't exist, then create it
	//		if (!file.exists()) {
	//			file.createNewFile();
	//		}
	//
	//		HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>();
	//		
	//	
	//
	//		int docCount = 0;
	//		int maxFeatId = 0;
	//		
	//		while ((p = docIdx.getNext()) != null) {
	//			docCount++;
	//			if (docCount % 50000 == 0) System.out.println(docCount);
	//			
	//			Map<Integer, Float> postings = p.getPostings();
	//
	//			for (Map.Entry<Integer, Float> posting : postings.entrySet()) {
	//				int featId = posting.getKey();
	//				//int count = Math.round(posting.getValue());
	//
	//				Integer oldCount = counts.get(featId);
	//				if (oldCount == null) oldCount = 0;
	//				counts.put(featId, oldCount + 1);
	//				
	//				if (featId > maxFeatId) maxFeatId = featId;
	//			}
	//
	//			//bw.write(Arrays.toString(relatedFeatures) + "\n");
	//		}
	//		
	//		FileWriter fw = new FileWriter(file.getAbsoluteFile());
	//		BufferedWriter bw = new BufferedWriter(fw);	
	//		for (int i = 0; i <= maxFeatId; i++) {
	//			if (counts.containsKey(i)) {
	//				bw.write(i + "\t" + counts.get(i) + "\n");
	//			}
	//		}
	//		bw.close();
	//
	//		docIdx.endTraversal();
	//
	//	}
	//
	//		public static void main(String[] args) {
	//			System.out.println("main");
	//			WikipediaDataSource data = new WikipediaDataSource();
	//	
	////			int blockId = 1;
	////			Configuration conf = new Configuration();
	////			FileSystem fs = null;
	////			SequenceFile.Reader reader = null;
	////	
	////			try {
	////				data.splitSequenceFile();
	////			} catch (IOException e) {
	////				// TODO Auto-generated catch block
	////				e.printStackTrace();
	////			}
	//	
	//			//		try {
	//			//			data.countFeatures();
	//			//		} catch (IOException e) {
	//			//			// TODO Auto-generated catch block
	//			//			e.printStackTrace();
	//			//		}
	//	
	//	
	//					try {
	//						data.dumpFeatures();
	//					} catch (IOException e) {
	//						// TODO Auto-generated catch block
	//						e.printStackTrace();
	//					}
	//			//		
	//			//				try {
	//			//					data.reorderSequenceFile();
	//			//				} catch (IOException e) {
	//			//					// TODO Auto-generated catch block
	//			//					e.printStackTrace();
	//			//				} 
	//	
	//			//				try {
	//			//					Path newFname = new Path("test_data/wikipedia/wikiRandom.seq");
	//			//		
	//			//					fs = FileSystem.get(conf);
	//			//					System.out.println(fname);
	//			//		
	//			//					reader = new SequenceFile.Reader(fs, newFname, conf);
	//			//		
	//			//					IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
	//			//					Writable val = (Writable) reader.getValueClass().newInstance();
	//			//		
	//			//					System.out.println(reader.getPosition());
	//			//					int size = 0;
	//			//					long startPos = reader.getPosition();
	//			//					long curPos = startPos;
	//			//					long lastPos = 0;
	//			//					int docCount = 0;
	//			//		
	//			//					HashMap<Integer, Float> blockFeatures = new HashMap<Integer, Float>();
	//			//		
	//			//					while (reader.next(key, val)) {
	//			//						curPos = reader.getPosition();
	//			//						size = (int) (curPos - startPos);
	//			//						docCount++;
	//			//						int docId = key.get();
	//			//						HashMap<Integer, Float> features = data.getFeaturesForDoc(docId);
	//			//		
	//			//						// Collect the features for each item into the block features aggregator
	//			//						for (Map.Entry<Integer, Float> entry : features.entrySet()) {
	//			//							Float value = blockFeatures.get(entry.getKey());
	//			//							if (value == null) {
	//			//								blockFeatures.put(entry.getKey(), entry.getValue());
	//			//							}
	//			//							else {
	//			//								blockFeatures.put(entry.getKey(), value + entry.getValue());
	//			//							}
	//			//						}
	//			//		
	//			//						// If we're over the size of the block, save it up to just before this item
	//			//						if (size > blockSize) {
	//			//							size = (int) (lastPos - startPos);
	//			//							System.out.println(blockId + " - " + startPos + ":" + (lastPos - 1) + " - " + size + ", Docs: " + docCount);
	//			//							FileOffset fo = new FileOffset(startPos, size);
	//			//							blockOffsetDict.put(blockId, fo);
	//			//		
	//			//							// Add the features to the block indexes
	//			//							PostingsList pl = new PostingsList(blockId, blockFeatures);
	//			//							blockFeaturesIdx.put(pl);
	//			//		
	//			//							// Now for the inverse index
	//			//							for (Map.Entry<Integer, Float> posting : blockFeatures.entrySet()) {
	//			//								int featId = posting.getKey();
	//			//								float value = posting.getValue();
	//			//		
	//			//								PostingsList featPost = new PostingsList(featId);
	//			//								featPost.addItem(blockId, value);	
	//			//								try {
	//			//									featureBlocksIdx.merge(featPost);
	//			//								} catch (ItemRetrievalException e) {
	//			//									// TODO Auto-generated catch block
	//			//									e.printStackTrace();
	//			//								}
	//			//							}
	//			//							featureBlocksIdx.syncDatabase();
	//			//							blockFeaturesIdx.syncDatabase();
	//			//							blockOffsetDict.syncDatabase();
	//			//		
	//			//							docCount = 0;
	//			//							blockId++;
	//			//							startPos = lastPos;
	//			//							blockFeatures = new HashMap<Integer, Float>();
	//			//		
	//			//						}
	//			//						lastPos = curPos;
	//			//					}
	//			//		
	//			//		
	//			//		
	//			//				} catch (IOException e) {
	//			//					// TODO Auto-generated catch block
	//			//					e.printStackTrace();
	//			//				} catch (InstantiationException e) {
	//			//					// TODO Auto-generated catch block
	//			//					e.printStackTrace();
	//			//				} catch (IllegalAccessException e) {
	//			//					// TODO Auto-generated catch block
	//			//					e.printStackTrace();
	//			//				}
	//		}

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
		String testIdFile = "test_data/logRegTestSetIds.txt";
		if (ExperimentParameters.check("numClasses", "2"))
			testIdFile = "test_data/logRegTestSetIds.txt.2class";
		else if (ExperimentParameters.check("numClasses", "6"))
			testIdFile = "test_data/logRegTestSetIds.txt.6class";

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

	public List<Integer> loadLabelIds() throws IOException {
		String binFile = "test_data/wikipedia/labels.bin";
		List<Integer> list = new ArrayList<Integer>();
		DataInputStream bin = new DataInputStream(new BufferedInputStream(new FileInputStream(binFile)));
		while (true) {
			try {
				list.add((int) bin.readShort());
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
		List<Integer> labelIds;
		List<Integer> goodIds = new ArrayList<Integer>();
		List<Integer> badIds = new ArrayList<Integer>();

		System.out.println("Loading ids");
		try {
			pageIds = loadDocIds();
			labelIds = loadLabelIds();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("Can't load page IDs.");
		}
		labeler.buildLabelCache(pageIds, labelIds);
		List<Item> itemList = new ArrayList<Item>();

		Map<String, Integer> labelCounts = new HashMap<String, Integer>();
		String[] labels = labeler.getLabels();
		for (String lbl : labels) {
			labelCounts.put(lbl, 0);
		}

		int maxMinorityCount = (int) (n * rarity);
		int maxMajorityCount = n - (labels.length - 1) * maxMinorityCount;

		int itemSortId = 1;

		List<Item> extras = new ArrayList<Item>();

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
					else if (label.equals("other") && labelCounts.get(label) >= maxMajorityCount) {
						extras.add(item);
						continue;
					}

					// we'll keep track of the good ids, for our heat map clusters.
					if (label.equals("other")) badIds.add(itemId);
					else goodIds.add(itemId);

					labelCounts.put(label, labelCounts.get(label) + 1);
				}
				item.setLabel(label);
			}

			item.setSortId(itemSortId);
			itemSortId++;	
			this.getFeaturesForDoc(itemId); // This is for heatmap calcs
			itemList.add(item);
		}

		while (itemList.size() < n && extras.size() > 0) {
			itemList.add(extras.remove(0));
			labelCounts.put("other", labelCounts.get("other") + 1);

		}

		Collections.shuffle(itemList, rand);
		System.out.println("Labels: " + labelCounts);

		if (ExperimentParameters.check("indexType", "heatMapClusters")) {
			loadHeatMapClusters(goodIds, badIds);
		}
		else if (ExperimentParameters.check("indexType", "synthClusters")) {
			loadSynthClusters(goodIds, badIds);
		}
		
		//computeClusterStats(goodIds, badIds);
		return itemList;
	}

	//@Override
	public List<Item> getItems2(int n) {
		Integer nextId = startBlock;
		List<Item> itemList = new ArrayList<Item>();

		Map<String, Integer> minorityCounts = new HashMap<String, Integer>();
		String[] labels = labeler.getLabels();
		for (String lbl : labels) {
			if (!lbl.equals("other")) {
				minorityCounts.put(lbl, 0);
			}
		}

		int itemSortId = 1;
		long loadTime = 0;
		while (itemList.size() < n) {
			DataBlock block = getBlockContaining(nextId);
			IntWritable key = new IntWritable();
			Writable contents = new Text();

			long pos = block.getPosition();
			long oldPos = -1;

			while (block.next(key, contents)) {
				long time1 = System.nanoTime();

				if (itemList.size() >= n) break;

				int itemId = key.get();

				// Make sure the items aren't included in the hold-out set
				if (testSet.contains(itemId)) continue;

				oldPos = pos;
				pos = block.getPosition();
				long offset = oldPos;
				long length = pos - oldPos;

				Item item = itemCache.get(itemId);
				if (rarity > 0) {

					String label = labeler.getLabel(itemId);

					if (minorityCounts.get(label) != null && minorityCounts.get(label) >= n * rarity) continue;
					if (minorityCounts.get(label) != null) minorityCounts.put(label, minorityCounts.get(label) + 1);

					item.setLabel(label);

				}

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
					// We've seen this item. It's a duplicate for some reason.
					continue;
				}
				long time2 = System.nanoTime();

				itemList.add(item);
				item.setLoadTime(time2 - time1);
			}

			nextId++;
		}
		Collections.shuffle(itemList, rand);
		System.out.println("Minority: " + minorityCounts);
		return itemList;
	}

	@Override
	public List<Item> getStoppingItems(int numPerClass, List<String> classNames) {
		List<Item> stoppingItems = new ArrayList<Item>();
		List<Integer> pageIds;
		List<Integer> labelIds;
		System.out.println("Loading ids");
		try {
			pageIds = loadDocIds();
			labelIds = loadLabelIds();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("Can't load page IDs.");
		}
		labeler.buildLabelCache(pageIds, labelIds);
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
		List<Integer> pageIds;
		List<Integer> labelIds;
		System.out.println("Loading ids");
		try {
			pageIds = loadDocIds();
			labelIds = loadLabelIds();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("Can't load page IDs.");
		}
		labeler.buildLabelCache(pageIds, labelIds);
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

	private void loadHeatMapClusters(List<Integer> inGoodIds, List<Integer> inBadIds) {
		List<Integer> goodIds = new ArrayList<Integer>(inGoodIds);
		List<Integer> badIds = new ArrayList<Integer>(inBadIds);
		double purity = Double.valueOf(ExperimentParameters.get("heatMapPurity"));
		double density = Double.valueOf(ExperimentParameters.get("heatMapDensity"));

		BetaDistribution beta = new BetaDistribution(purity * 20, 20 - (purity * 20));

		System.out.println("Loading Heat Map Clusters: P = " + purity + ", D = " + density);
		Collections.shuffle(badIds);

		// HACK: Assume 1 million items, 5000 clusters
		int numItems = 1000000;
		int perGoodCluster = 100;	

		// Also HACK: Assume 5000 per class, so let's add 5000 bad items 
		// to the good items so we have a balanced set.
		for (int i = 0; i < 500; i++) {
			goodIds.add(badIds.remove(0));
		}

		Collections.shuffle(goodIds);

		System.out.println("Good IDs: " + goodIds.size() + ", Bad IDs: " + badIds.size());

		// purity means the percentage of good items in the cluster.
		int goodPerCluster = (int) Math.floor(purity * perGoodCluster);
		int numGoodClusters = (int) Math.floor((double) goodIds.size()/goodPerCluster);

		// first, build the good clusters
		//for (int i = 0; i < numGoodClusters; i++) {
		int n = 0;
		numGoodClusters = 0;
		while (goodIds.size() > 0) {
			int clusterId = ++n;
			int remaining = perGoodCluster; // total number of items in a good cluster

			// put the good ids in the good clusters
			goodPerCluster = (int) Math.ceil(beta.sample() * perGoodCluster);
			int goodInThisCluster = 0;
			for (int j = 0; j < goodPerCluster; j++) {
				if (goodIds.size() == 0) continue;

				int docId = goodIds.remove(0);

				HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
				feature.put(clusterId, (float) 1.0);
				featureCache.put(docId, feature);
				remaining--;
				goodInThisCluster++;
			}

			int badInThisCluster = 0;
			// fill it up with enough bad ids to get to the proper purity
			while (remaining > 0 && badIds.size() > 0) {
				int docId = badIds.remove(0);
				HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
				feature.put(clusterId, (float) 1.0);
				featureCache.put(docId, feature);
				remaining--;
				badInThisCluster++;
			}

			System.out.println("good=" + goodInThisCluster + ", bad="+ badInThisCluster);

			if (goodPerCluster > 0) numGoodClusters++;
		}

		// density is the percentage of clusters that are GOOD
		int numClusters = (int) Math.round((double) numGoodClusters/density);
		int numBadClusters = numClusters - numGoodClusters;
		System.out.println("numClusters = " + numClusters + ", numGoodClusters = " + numGoodClusters + " good remaining = " + goodIds.size());

		// we may have a few good ids left over. Let's throw them in with the bad ids.
		badIds.addAll(goodIds);
		Collections.shuffle(badIds);

		inGoodIds.removeAll(goodIds);
		inBadIds.addAll(goodIds);

		int firstBadClusterId = numGoodClusters + 1;

		if (numBadClusters < 1) numBadClusters = 1;

		// now build the bad clusters
		while (badIds.size() > 0) {
			for (int i = 0; i < numBadClusters; i++) {
				if (badIds.size() > 0) {
					int clusterId = firstBadClusterId + i;
					int docId = badIds.remove(0);
					HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
					feature.put(clusterId, (float) 1.0);
					featureCache.put(docId, feature);
				}
			}
		}
	}

	private void loadSynthClusters(List<Integer> inGoodIds, List<Integer> inBadIds) {
		List<Integer> goodIds = new ArrayList<Integer>(inGoodIds);
		List<Integer> badIds = new ArrayList<Integer>(inBadIds);

		Collections.shuffle(badIds);
		Collections.shuffle(goodIds);
		
		// coverage 
		double coverage = Double.valueOf(ExperimentParameters.get("clusterCoverage"));

		int numClusters = 500;
		int numGoodClusters = (int) Math.floor(coverage * numClusters);
		
		List<List<Integer>> clusters = new ArrayList<List<Integer>>();
		
		for (int i = 0; i < numClusters; i++) clusters.add(new ArrayList<Integer>());
		
		while (goodIds.size() > 0) {
			for (int clusterId = 0; clusterId < numGoodClusters; clusterId++) {
			
				int docId = goodIds.remove(0);

				HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
				feature.put(clusterId, (float) 1.0);
				featureCache.put(docId, feature);
				
				if (goodIds.size() == 0) break;
			}
		}
		
		while (badIds.size() > 0) {
			for (int clusterId = 0; clusterId < numClusters; clusterId++) {
				int docId = badIds.remove(0);

				HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
				feature.put(clusterId, (float) 1.0);
				featureCache.put(docId, feature);
				
				if (badIds.size() == 0) break;
			}
		}

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

	private void loadLSHFeatures() {
		try {
			String clusterFile = ExperimentParameters.get("clusterFile", "none.txt");

			BufferedReader br = new BufferedReader(new FileReader(clusterFile));
			String line;
			System.out.println("Loading features from: " + clusterFile);

			while ((line = br.readLine()) != null) {
				String[] vals = line.trim().split("\t");
				int clusterId = Integer.valueOf(vals[0]);
				String[] docIds = vals[1].split(",");

				for (String idStr : docIds) {
					int docId = Integer.valueOf(idStr);
					HashMap<Integer, Float> feature = new HashMap<Integer, Float>();
					feature.put(clusterId, (float) 1.0);
					featureCache.put(docId, feature);
				}
			}

			br.close();
		} catch (Exception e) {
			throw new RuntimeException("Error reading cluster file: " + e.getMessage());
		}
	}

	private void computeClusterStats(List<Integer> goodIds, List<Integer> badIds) {
		Map<Integer, List<Integer>> clusters = new HashMap<Integer, List<Integer>>();
		Map<Integer, Integer> clustGood = new HashMap<Integer, Integer>();
		Map<Integer, Integer> clustBad = new HashMap<Integer, Integer>();

		double N = 0;
		double totalGood = 0;
		double totalBad = 0;

		for (int id : goodIds) {
			HashMap<Integer, Float> features = this.getFeaturesForDoc(id);// featureCache.get(id);
			for (Map.Entry<Integer, Float> feature : features.entrySet()) {
				int clusterId = feature.getKey();
				if (!clusters.containsKey(clusterId)) {
					clusters.put(clusterId, new ArrayList<Integer>());
					clustGood.put(clusterId, 0);
					clustBad.put(clusterId, 0);
				}
				clusters.get(clusterId).add(1);
				clustGood.put(clusterId, clustGood.get(clusterId) + 1);
				N++;
				totalGood++;
			}
		}

		for (int id : badIds) {
			HashMap<Integer, Float> features = this.getFeaturesForDoc(id);//featureCache.get(id);
			for (Map.Entry<Integer, Float> feature : features.entrySet()) {
				int clusterId = feature.getKey();
				if (!clusters.containsKey(clusterId)) {
					clusters.put(clusterId, new ArrayList<Integer>());
					clustGood.put(clusterId, 0);
					clustBad.put(clusterId, 0);
				}
				clusters.get(clusterId).add(0);
				clustBad.put(clusterId, clustBad.get(clusterId) + 1);
				N++;
				totalBad++;
			}
		}

		double density = 0.0;
		double purity = 0.0;
		double sumPurity = 0.0;
		int numGood = 0;

		double H_CK = 0.0; // Homogeneity(C|K)
		double H_C = 0.0; // Homogeneity(C)
		double H_KC = 0.0;
		double H_K = 0.0;

		double probTwoInARow = 0.0;
		double probTwoOfThree = 0.0;
		
		//double N = goodIds.size() + badIds.size();
		List<Double> purities = new ArrayList<Double>();
		List<Double> noises = new ArrayList<Double>();
		Map<String, Integer> purityBuckets = new HashMap<String,Integer>();

		FileWriter fw;
		try {
			fw = new FileWriter("cluster_data.txt");

			BufferedWriter bw = new BufferedWriter(fw);	
			for (Map.Entry<Integer, List<Integer>> cluster : clusters.entrySet()) {
				int id = cluster.getKey();
				int size = cluster.getValue().size();
				int sum = 0;

				for (int val : cluster.getValue()) {
					sum += val;
				}


				bw.write(sum + "\t" + size + "\n");

				double clusterPurity = (double) sum/size;
				
				double goodWeight = (double) sum/totalGood;
				purity += (double) goodWeight * clusterPurity;
				sumPurity += clusterPurity;

				if (size > 1) {
					probTwoInARow += clusterPurity * (double) (sum - 1)/(size - 1);
					probTwoOfThree += (clusterPurity * clusterPurity) + (clusterPurity * (1 - clusterPurity) * clusterPurity);
				}
				
				if (sum > 0) {
					String purityLabel = String.format("%.6f", (double) goodWeight * sum/size);

					if (!purityBuckets.containsKey(purityLabel)) {
						purityBuckets.put(purityLabel, 0);
					}
					purityBuckets.put(purityLabel, purityBuckets.get(purityLabel) + 1);
					purities.add((double) sum/size);
					noises.add( 1.0 - (double) sum/size);
					numGood++;
					density +=size/N;// (1.0/clusters.size()) * 
					//System.out.println(sum + "\t" + size);
				}

				// Homogeneity
				double goodCnt = clustGood.get(id);
				double badCnt = clustBad.get(id);

				if (goodCnt > 0) H_CK += (goodCnt/N) * Math.log(goodCnt/size);
				if (badCnt > 0) H_CK += (badCnt/N) * Math.log(badCnt/size);

				// Completeness
				if (goodCnt > 0) H_KC += (goodCnt/N) * Math.log(goodCnt/totalGood);
				if (badCnt > 0) H_KC += (badCnt/N) * Math.log(badCnt/totalBad);
				H_K += (size/N) * Math.log(size/N);

			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		H_CK = -H_CK;
		H_C = -(totalGood/N * Math.log(totalGood/N) + totalBad/N * Math.log(totalBad/N));
		H_KC = -H_KC;
		H_K = -H_K;
		double homogeneity = 1 - H_CK/H_C;
		double completeness = 1 - H_KC/H_K;
		double V_score = 2 * homogeneity * completeness/(homogeneity + completeness);
		
		//purity /= numGood;
		//density = (double) numGood/clusters.size();
		System.out.println("Num Good: " + numGood );
		System.out.println("Purity = " + purity + "  Density = " + density);
		System.out.println("Purity * Density = " + (purity * density));
		System.out.println("Prob of picking a good item: " + sumPurity/((double) clusters.size()) + " sumPurity: " + sumPurity + " clusters: " + clusters.size());
		System.out.println("Prob of picking 2 in a row: " + probTwoInARow/((double) clusters.size()));
		System.out.println("Prob of picking 2 with 3: " + probTwoOfThree/((double) clusters.size()));
		
		System.out.println("-----");

		double meanPurity = getMean(purities);
		double stdPurity = getStdDev(noises);
		double snr = meanPurity/stdPurity;
		double medianPurity = getMedian(purities);
		double skew = (meanPurity - medianPurity)/getStdDev(purities);

		System.out.println("Mean purity: " + meanPurity);
		System.out.println("Median purity: " + medianPurity);
		System.out.println("StdDev Noise: " + stdPurity);
		System.out.println("SNR: " + snr);
		System.out.println("Skew: " + skew);


		System.out.println("-----");
		System.out.println("H_CK = " + H_CK + "  H_C = " + H_C);
		System.out.println("Homogeneity = " + homogeneity);
		System.out.println("H_KC = " + H_KC + "  H_K = " + H_K);
		System.out.println("Completeness = " + completeness);
		System.out.println("V_score = " + V_score);
		System.out.println("-----");

//		for (Map.Entry<String, Integer> bucket : purityBuckets.entrySet()) {
//			System.out.println(bucket.getKey() + "," + bucket.getValue());
//		}

		// Now we will simulate how long it will take to find a good item
		List<Integer> stepList = new ArrayList<Integer>();
		for (int trial = 0; trial < 50; trial++) {
			int steps = 0;
			List<Integer> idList = new ArrayList<Integer>();
			Map<Integer, List<Integer>> copy = new HashMap<Integer, List<Integer>>();

			// first make a copy
			for (Map.Entry<Integer, List<Integer>> cluster : clusters.entrySet()) {
				List<Integer> l = new ArrayList<Integer>(cluster.getValue());
				Collections.shuffle(l);
				copy.put(cluster.getKey(), l);
				idList.add(cluster.getKey());
			}

			Collections.shuffle(idList);
			boolean foundOne = false;

			while (!foundOne && idList.size() > 0) {
				int id = idList.remove(0);
				int item = copy.get(id).remove(0);

				steps++;

				if (item > 0) foundOne = true;

				if (copy.get(id).size() > 0) idList.add(id);
			}

			stepList.add(steps);
		}
		System.out.println("Steps until first minority item:\n" + stepList);
	}

	double getMean(List<Double> data)
	{
		double sum = 0.0;
		for(double a : data)
			sum += a;
		return sum/data.size();
	}

	double getVariance(List<Double> data)
	{
		double mean = getMean(data);
		double temp = 0;
		for(double a :data)
			temp += (mean-a)*(mean-a);
		return temp/data.size();
	}

	double getStdDev(List<Double> data)
	{
		return Math.sqrt(getVariance(data));
	}

	public double getMedian(List<Double> data) 
	{
		List<Double> b = new ArrayList<Double>(data);

		Collections.sort(b);

		if (data.size() % 2 == 0) 
		{
			return (b.get((b.size() / 2) - 1) + b.get(b.size() / 2)) / 2.0;
		} 
		else 
		{
			return b.get(b.size() / 2);
		}
	}

}
