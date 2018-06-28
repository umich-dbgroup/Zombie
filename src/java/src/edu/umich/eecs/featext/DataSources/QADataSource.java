package edu.umich.eecs.featext.DataSources;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import edu.umich.eecs.featext.harness.ExperimentParameters;
import edu.umich.eecs.featext.index.DocumentFeaturesIndex;
import edu.umich.eecs.featext.index.FeatureDocumentsIndex;
import edu.umich.eecs.featext.index.FeatureNameDict;
import edu.umich.eecs.featext.index.FileOffset;
import edu.umich.eecs.featext.index.FileOffsetDict;
import edu.umich.eecs.featext.index.ItemRetrievalException;
import edu.umich.eecs.featext.index.PostingsList;

public class QADataSource implements DataSource {
	public static final String ID_STRING = "qaData";

	public String questionFile = "test_data/countries/questions.txt";

	static Path fname = new Path("test_data/wikipedia/wikiRandom.seq");
	static Path oldFname = new Path("test_data/wikipedia/wiki.seq");

	static DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex("wiki500DocIdx");
	static FeatureDocumentsIndex featDocsIdx = new FeatureDocumentsIndex("wiki500InvIdx");
	static FeatureNameDict featureIdToName = new FeatureNameDict("wiki500FeatureNameDict");

	static FileOffsetDict offsetDict = new FileOffsetDict("offsetDict");

	static FileOffsetDict blockOffsetDict = new FileOffsetDict("wikiRandomBlockOffsets");
	static DocumentFeaturesIndex blockFeaturesIdx = new DocumentFeaturesIndex("wikiRandomBlockFeatures");
	static FeatureDocumentsIndex featureBlocksIdx = new FeatureDocumentsIndex("wikiRandomFeatureBlocks");

	private HashMap<Integer, HashMap<Integer, Float>> featureCache = new HashMap<Integer, HashMap<Integer, Float>>();

	private HashMap<String, Integer> titleToId = new HashMap<String, Integer>();

	private ArrayList<Integer> newIds = new ArrayList<Integer>();
	private HashMap<Integer, ArrayList<Integer>> newFeatureDocs = new HashMap<Integer, ArrayList<Integer>>();
	private HashMap<Integer, ArrayList<Integer>> newDocFeatures = new HashMap<Integer, ArrayList<Integer>>();

	private HashMap<String, Integer> newFeaturesNamesToIds = new HashMap<String, Integer>();
	
	static public long blockSize = 5242880; // bytes

	public static ItemCache itemCache = new ItemCache();

	private ArrayList<Question> questions = new ArrayList<Question>();

	private Labeler labeler = new QALabeler();

	private ArrayList<Item> itemList;
	double rarity = 0.001;
	
	public QADataSource() {
		try {
			loadQuestionFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (ExperimentParameters.get("rarity") != null) {
			rarity = Double.valueOf(ExperimentParameters.get("rarity"));
		}
	}

	@Override
	public HashMap<Integer, Float> getFeaturesForDoc(int docId) {
		PostingsList pl;
		HashMap<Integer, Float> hm = featureCache.get(docId);

		Question question = questions.get(docId);

		if (hm == null) {
			try {
				hm = new HashMap<Integer, Float>();
				Set<Integer> featSet = new HashSet<Integer>();
				
				WikiPage[] wp = new WikiPage[question.getEntities().length];
				int i = 0;
				for (int entityId : question.getEntities()) {
					pl = docIdx.get(entityId);
					HashMap<Integer, Float> feats = pl.getPostings();

					if (featSet.size() == 0) {
						featSet.addAll(feats.keySet());
					}
					else {
						featSet.retainAll(feats.keySet());
					}
					
					try {
						wp[i++] = new WikiPage(entityId);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				// We're assuming there are 2 wikipages
				boolean firstHasSecond = false;
				boolean secondHasFirst = false;
				
				if (wp[0].getText().contains(wp[1].getTitle())) {
					firstHasSecond = true;
					Integer featId = getNewFeatureId("firstHasSecond");
					hm.put(featId, (float) 1.0);
				}
				
				if (wp[1].getText().contains(wp[0].getTitle())) {
					secondHasFirst = true;
					Integer featId = getNewFeatureId("secondHasFirst");
					hm.put(featId, (float) 1.0);
				}
				
				if (firstHasSecond && secondHasFirst) {
					Integer featId = getNewFeatureId("bothHaveBoth");
					hm.put(featId, (float) 1.0);
				}
				
				int matchCount = 0;
				for (Integer featId : featSet) {
					hm.put(featId, (float) 1.0);
					
					// Add a feature for each total of matches
					matchCount++;
					String newFeatName = "matchTotal:"+matchCount;
					Integer newFeatureId = getNewFeatureId(newFeatName);
					
				//	hm.put(newFeatureId, (float) 1.0);					
				}

				featureCache.put(docId, hm);
				
				// Add everything to arm 0
				hm.put(0, (float) 1.0);
				
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
			pl = featDocsIdx.get(featureId);
			hm = pl.getPostings();
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
		return name;
	}

	@Override
	public List<Integer> getBlockIds() {
		ArrayList<Integer> ids = new ArrayList<Integer>();
		for (int i = 1; i <= 1; i++) {
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
			//block = (DataBlock) new FSDataBlock(fname, fo.getOffset(), blockSize);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return block;
	}

	public Item getItem(int itemId) {
		Item item = itemCache.get(itemId);

		if (!item.isInitialized()) {
			String contents = questions.get(itemId-1).getQuestion() + "\n" + 
							  questions.get(itemId-1).getAnswer();

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

	private void loadQuestionFile() throws IOException {
		// Open the file
		FileInputStream fstream = new FileInputStream(questionFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream, "UTF8"));

		String strLine;

		//Read File Line By Line
		while ((strLine = br.readLine()) != null)   {
			// Print the content on the console
			questions.add(new Question(questions.size() + 1, strLine.trim()));
		}

		//Collections.shuffle(questions);
		
		((QALabeler) labeler).setQuestions(questions);
		
		//Close the input stream
		br.close();
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

	public ArrayList<Question> getQuestions() {
		return questions;
	}

	@Override
	public Labeler getLabeler() {
		return labeler;
	}

	@Override
	public List<Item> getItems(int n) {
		
		itemList = new ArrayList<Item>();
		int count = 0;
		int i = 0;
		while (itemList.size() < n) {
			Question q = questions.get(i);
			i++;
			if (q.getLabel().equals("correct")) {
				if (count >= n * rarity) continue;
				else count++;
			}
			Item item = getItem(q.questionId);
			itemList.add(item);
		}
		Collections.shuffle(itemList);
		return itemList;
	}

	@Override
	public List<Item> getTestItems(int numPerClass, List<String> classNames) {
		List<Item> items = new ArrayList<Item>();
		int wrongCount = 0;
		int rightCount = 0;
		for (int i = questions.size() - 20; i > 0; i--) {
			Question q = questions.get(i);
			//System.out.println(q.getQuestion());
			if (q.getLabel().equals("correct") && rightCount < numPerClass) {
				rightCount++;
				items.add(getItem(q.questionId));
			}
			else if (q.getLabel().equals("wrong") && wrongCount < numPerClass) {
				wrongCount++;
				items.add(getItem(q.questionId));
			}
			
			if (wrongCount >= numPerClass && rightCount >= numPerClass) {
				break;
			}
		}
		System.out.println("Test Size: " + items.size());
		return items;
	}

	@Override
	public ItemCache getItemCache() {
		// TODO Auto-generated method stub
		return null;
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

	
	
}
