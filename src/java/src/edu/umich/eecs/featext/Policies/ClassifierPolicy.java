/**
 * 
 */
package edu.umich.eecs.featext.Policies;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import weka.classifiers.bayes.NaiveBayesUpdateable;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;
import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.DataSources.WikiRegressionLabeler;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.UDFs.UDF;
import edu.umich.eecs.featext.harness.ExperimentParameters;

/**
 * @author Mike Anderson
 *
 */
public class ClassifierPolicy implements Policy {
	DataSource data;
	LearningTask ltask;
	UDF udf;
	Path fname;

	private ArrayList<Integer> allBlocks;
	private HashSet<Integer> usedBlocks;

	public static final String ID_STRING = "classifier";

	private Set<Integer> excludeIds;

	private static int testSize;
	private static int startBlock;

	private boolean armsAreLoaded = false;

	private int itemsInQueues = 0;

	// Here are some parameters
	int initialSeedCount = 10; // Number of random documents to examine before 
	// we start querying arms.
	private String[] labels =  {"yes", "no"};
	private HashMap<Integer, Integer> featureMap;
	private ArrayList<Integer> featureList;
	private ArrayList<Integer> trainIds;
	private ArrayList<Integer> newTrainIds;

	private Instances dataset;
	private ArrayList<Item> allItems = new ArrayList<Item>();
	private List<Item> unclassifiedItems = new ArrayList<Item>();
	private ArrayList<Item> classifiedItems = new ArrayList<Item>();
	private HashMap<Integer, Integer> featureLocs;
	private NaiveBayesUpdateable cModel;

	private HashMap<Integer, HashMap<Integer, Float>> featureCache = new HashMap<Integer, HashMap<Integer, Float>>();
	private Attribute cls;
	private ArrayList<Attribute> attributes;

	HashMap<String, Integer> labelCounts = new HashMap<String, Integer>();
	private int lastRecordId;

	HashMap<Integer, Boolean> itemRelevance = new HashMap<Integer, Boolean>();
	
	private int seenSinceTrain = 0;
	private int trainingThreshold = Integer.valueOf(ExperimentParameters.get("trainingThreshold", 1000));

	/**
	 * <code>createPolicy</code> returns an instance of the
	 * policy with the given label.  Right now there's only
	 * one.
	 */
	public static Policy createPolicy(String policyId) {
		String[] args = policyId.split("-");
		if (args.length == 3 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
			startBlock = Integer.parseInt(args[2]) * 100;

			startBlock = 517;
		} else if (args.length == 2 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
			startBlock = 0;
			//startBlock = rand.nextInt(1200); // number of wiki blocks
		}
		else {
			testSize = 2000;
			startBlock = 0;

			//startBlock = rand.nextInt(1200); // number of wiki blocks
		}
		return new ClassifierPolicy(policyId);
	}

	public ClassifierPolicy(String policyId) {}

	@Override
	public void init(DataSource data, LearningTask ltask, Path fname) {
		this.data = data;
		this.ltask = ltask;
		this.fname = fname;

		System.out.println("Training threshold: " + trainingThreshold);
		
		excludeIds = ltask.getTestSet();

		List<Integer> blockIds = data.getBlockIds();

		// Clear out all the IDs that are to be excluded.
		// These are probably in a test set for a learning algorithm.
		//		for (Integer id : excludeIds) {
		//			docIds.remove(id);
		//		}

		Collections.shuffle(blockIds);

		allBlocks = new ArrayList<Integer>(blockIds);
		usedBlocks = new HashSet<Integer>();

		// New classifier stuff here


		featureMap = new HashMap<Integer, Integer>();
		trainIds = new ArrayList<Integer>();
		newTrainIds = new ArrayList<Integer>();
		featureList = new ArrayList<Integer>();

		labelCounts = new HashMap<String, Integer>();
		for (String lbl : labels) {
			labelCounts.put(lbl, 0);
		}
		
		if (!armsAreLoaded) {

			loadItems();

		}

	}

	@Override
	public DataBlock getNextDataBlock() {
		processPrevBlock();

		Item item = null;

		// We go through the unclassified items, classifying each one until we find a
		// "yes" item and then we use it. Once we run out of unclassified items, the
		// rest in allItems should all be "no" items as predicted by the classifier.
		if (unclassifiedItems.size() == 0) {
			item = allItems.get(0);
		} else {
			ArrayList<Item> toRemove = new ArrayList<Item>();
			//for (Item nextItem : unclassifiedItems) {
            while (unclassifiedItems.size() > 0) {
				item = unclassifiedItems.remove(unclassifiedItems.size() - 1);

				// Find the item of the desired label with the highest probability

				if (classifyItem(item.getItemId()).equals("yes")) {
					break;
				}
			}
			//System.out.println("Examined " + toRemove.size() + " of " + unclassifiedItems.size());

			//unclassifiedItems.removeAll(toRemove);
		}
		
		DataBlock nextBlock = null;

		newTrainIds.add(item.getItemId());
		allItems.remove(item);

		lastRecordId = item.getItemId();

		item.setDataSource(data);
		
		nextBlock = new ItemDataBlock(item); 

		return nextBlock;
	}

	public void processPrevBlock() {
		// Do this if we've already sent a block to process. We start building the
		// policy's training set after the learning task has processed a record.
		if (lastRecordId > 0 && seenSinceTrain < trainingThreshold) {	
			Map<Integer, Double> rewards = ltask.getAllItems();

			for (Map.Entry<Integer, Double> rewardEntry : rewards.entrySet()) {
				Integer itemId = rewardEntry.getKey();
				double reward = rewardEntry.getValue();

				// Consider it relevant if the reward is > 0.5
				boolean isRelevant = (reward > 0.5);

				itemRelevance.put(itemId, isRelevant);	
				
				seenSinceTrain++;

				// If we've seen enough items, retrain the classifier
				if (seenSinceTrain == trainingThreshold) {
					updateTrainingSet();

					unclassifiedItems = new ArrayList<Item>(allItems);
					//seenSinceTrain = 0;
				}   
			}
		}
	}

	private DataBlock findBestBlock() {
		DataBlock nextBlock = null;

		if (allBlocks.size() == 0) {
			return nextBlock; // null
		}
		else { // (!sawPositive || rand.nextDouble() <= epsilon || arms.size() == 0 || allBlocks.size() < 3) {
			Integer nextId = this.allBlocks.get(startBlock);

			nextBlock = data.getBlockContaining(nextId);

			this.allBlocks.remove(nextId);
			this.usedBlocks.add(nextId);

			initialSeedCount--;
		}

		return nextBlock;
	}

	private void updateTrainingSet() {
		if (cModel == null) {
			buildTrainingSet();
		}
		else {
			System.out.println("Updating Classifier with " + newTrainIds.size() + " items");

			for (int id : this.newTrainIds) {
				try {
					cModel.updateClassifier(createInstance(id));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		trainIds.addAll(newTrainIds);
		newTrainIds = new ArrayList<Integer>();
	}
	
	private void buildTrainingSet() {
		labelCounts = new HashMap<String, Integer>();
		for (String lbl : labels) {
			labelCounts.put(lbl, 0);
		}

		cModel = new NaiveBayesUpdateable();

		// -S 6 for L1-regularized Logistic Regression
		String[] options = {};//{"-S", "6", "-C", "1.0"};

		try {
			cModel.setOptions(options);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		

		this.dataset = new Instances("Dataset", attributes, 0);
		this.dataset.setClass(cls);

		// Now go through IDs in the training set and actually add them as
		// example instances with the temporary feature IDs figured out above.
		System.out.println("Building training set with " + newTrainIds.size() + " items");
		for (int id : this.newTrainIds) {
			dataset.add(createInstance(id));
		}

		//reweightTrainingSet();

		try {
			cModel.buildClassifier(dataset);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private Instance createInstance(int id) {
		HashMap<Integer, Float> features;

		double[] values = new double[this.dataset.numAttributes()];
		//features = featureCache.get(id);
		//if (features == null) {
			features = data.getFeaturesForDoc(id);
			//featureCache.put(id, features);
		//}

		for (Map.Entry<Integer, Float> entry : features.entrySet()) {
			if (featureLocs.containsKey(entry.getKey())) {
				values[featureLocs.get(entry.getKey())] = 1.0; //entry.getValue(); // Could use some other value.
			}
		}
		
		
		Instance inst = new SparseInstance(1.0, values);
		inst.setDataset(this.dataset);		

		Boolean isRelevant = itemRelevance.get(id);
		if (isRelevant == null) isRelevant = false;

		String lbl = "no";

		if (isRelevant) {
			lbl = "yes";
		}

		labelCounts.put(lbl, labelCounts.get(lbl) + 1);

		inst.setClassValue(lbl);
		
		return inst;
	}

	private String classifyItem(int id) {
		String className = null;

		double[] values = new double[this.dataset.numAttributes()];

		HashMap<Integer, Float> features = featureCache.get(id);
		if (features == null) {
			features = data.getFeaturesForDoc(id);
			//featureCache.put(id, features);
		}

		for (Map.Entry<Integer, Float> entry : features.entrySet()) {

			//if (selectedFeatures.get(entry.getKey()) != null) {
			if (featureLocs.containsKey(entry.getKey())) {
				values[featureLocs.get(entry.getKey())] = 1.0; //entry.getValue(); // Could use some other value.
			}
		}

		Instance inst = new SparseInstance(1.0, values);
		inst.setDataset(dataset);		

		try {
			int classValue = (int) cModel.classifyInstance(inst);
			className = labels[classValue];
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return className;
	}


	private void loadItems() {
		List<Item> itemList = new LinkedList<Item>(data.getItems(testSize));

		int mislabelArmCount = Integer.valueOf(ExperimentParameters.get("mislabelArmCount", "0"));
		Set<Integer> mislabelIds = new HashSet<Integer>();		

		if (mislabelArmCount > 0) {
			try {
				String inFileName = "mislabel_" + ExperimentParameters.get("corpusSize") + 
						"_" + mislabelArmCount + "_" + ExperimentParameters.get("testRunNumber") + ".txt";
				Scanner s = new Scanner(new File(inFileName));
				while (s.hasNext()){
					int itemId = Integer.valueOf(s.next());
					mislabelIds.add(itemId);
					if (ExperimentParameters.check("noisyFeatures", "true")) data.getItemCache().get(itemId).setShouldRandomizeFeatures(true);	
				}
				s.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			System.out.println("Mislabeled item count: " + mislabelIds.size());
		}
		Labeler labeler = data.getLabeler();
		if (mislabelIds.size() > 0) ((WikiRegressionLabeler) labeler).setMislabelSet(mislabelIds, 1.0);
		
		for (Item item : itemList) {
			int itemId = item.getItemId();

			if (excludeIds.contains(itemId)) continue;

			itemsInQueues++;

			allItems.add(item);
			
			// Build a catalog of the features
			HashMap<Integer, Float> features;
			features = data.getFeaturesForDoc(itemId);

			for (Map.Entry<Integer, Float> entry : features.entrySet()) {
				int fId = entry.getKey();	
				float fVal = entry.getValue();
				if (fVal > 0) {
					if (this.featureMap.containsKey(fId)) {
						this.featureMap.put(fId, this.featureMap.get(fId) + 1);
					}				
					else {
						this.featureMap.put(fId, 1);
					}
				}
			}			
		}

		// Determine which feature we want to use in the classifier
		ArrayList<String> classes = new ArrayList<String>();
		classes.add("yes");
		classes.add("no");
		cls = new Attribute("class", classes);

		attributes = new ArrayList<Attribute>();
		featureLocs = new HashMap<Integer, Integer>();
		featureList = new ArrayList<Integer>();

		int loc = 0;
		for (Map.Entry<Integer, Integer> entry : this.featureMap.entrySet()) {
			// leave out rare features
			if (entry.getValue() >= (double) allItems.size()*0.2) {
				attributes.add(new Attribute(String.valueOf(entry.getKey())));
				featureLocs.put(entry.getKey(), loc++);
				this.featureList.add(entry.getKey());
			}
		}
		
		attributes.add(cls);

		// Shuffle the items, so we get a random selection
		Collections.shuffle(allItems);
		armsAreLoaded = true;
	}

	@Override
	public double[] getOutput() {
		double[] vals =  {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		return vals;
	}

	public void close() {
	}

}
