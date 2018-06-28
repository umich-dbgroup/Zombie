/**
 * 
 */
package edu.umich.eecs.featext.Policies;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.MatrixFeatures;

import com.google.common.base.Joiner;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.DataSources.WikiLabeler;
import edu.umich.eecs.featext.DataSources.WikiRegressionLabeler;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.ProcessingResults;
import edu.umich.eecs.featext.UDFs.UDF;
import edu.umich.eecs.featext.harness.ExperimentParameters;

/**
 * @author Mike Anderson
 *
 */
public class SerialContextualBanditPolicy implements Policy {
	private int testSize;
	DataSource data;
	LearningTask ltask;
	UDF udf;
	Path fname;

	private ItemCache itemCache;

	private List<Item> itemList;

	public static final String ID_STRING = "synth_real_contextual_bandit";

	private Set<Integer> excludeIds;

	private int lastRecordId = 0;
	private double lastEstimate;

	private boolean armsAreLoaded = false;

	private DenseMatrix64F learnerCxt;
	private DenseMatrix64F lastCxt;

	//PrintWriter pw;

	Random rand = new Random();

	// Here are some parameters
	int initialSeedCount = 10; // Number of random documents to examine before 
	// we start querying arms.

	int numStartPerClass = 1; // Number of items needed per class to end random exploration	

	double epsilon = 0.0;//

	double minorityPct = 0.001;

	int runId = 0;

	List<Double> rankings = new ArrayList<Double>();
	//	private List<Double> bestActuals = new ArrayList<Double>();

	/******** New stuff for Real Bandit *******/
	Map<Integer, Double> itemsSeenRewards = new HashMap<Integer, Double>();
	Map<Integer, DenseMatrix64F> itemsSeenContexts = new HashMap<Integer, DenseMatrix64F>();

	Set<Integer> initialItems = new HashSet<Integer>();

	QueueArm armPulled;
	private List<QueueArm> armList;
	private List<QueueArm> initialArmList;
	private Queue<QueueArm> armHeap;

	private boolean doRandExplore;
	private boolean doInitialSearch = false;
	private boolean doArmScan = false;
	private int processedCount = 0;

	private boolean armsSortedIncreasing = false;
	private double currentAccuracy;

	double maxItemSize = Double.NEGATIVE_INFINITY;
	double minItemSize = Double.POSITIVE_INFINITY;

	int cacheHits = 0;

	/**
	 * <code>createPolicy</code> returns an instance of the
	 * policy with the given label.  Right now there's only
	 * one.
	 */
	public static Policy createPolicy(String policyId) {
		return new SerialContextualBanditPolicy(policyId);
	}

	public SerialContextualBanditPolicy(String policyId) {
		String[] args = policyId.split("-");
		if (args.length == 3 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
		} else if (args.length == 2 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
		}
		else {
			testSize = 100000;
		}	

		//rand.setSeed(310);
	}

	@Override
	public void init(DataSource data, LearningTask ltask, Path fname) {
		this.data = data;
		this.ltask = ltask;
		this.fname = fname;

		excludeIds = ltask.getTestSet();

		itemCache = data.getItemCache();

		lastCxt = new DenseMatrix64F(ltask.getContext().toArray2());

		//		if (ltask.getContextType().equals("decisionBoundary")) armsSortedIncreasing = false;
		//		else armsSortedIncreasing = true;

		if (!armsAreLoaded) {
			System.out.print("Loading arms: ");
			long time1 = System.nanoTime();
			loadArms();
			long time2 = System.nanoTime();
			System.out.print((double)(time2 - time1)/1000000 + " ms\n");

			// Sort itemList in the same (random) order as the arms
			Collections.sort(itemList, new Comparator<Item>(){
				public int compare(Item item1, Item item2) {
					return Double.compare(item1.getSortId(), item2.getSortId());
				}
			});			

			pruneArms();			
		}
	}

	@Override
	public DataBlock getNextDataBlock() {	
		DataBlock nextBlock = null;

		Item item = null;
		//armPulled = null;

		if (doInitialSearch) {
			// TODO: this is very task-specific. Remove or generalize. 
			int[] labelCounts = ltask.getCounts();

			doRandExplore = false;
			doInitialSearch = false;
			for (int cnt : labelCounts) {
				if (cnt < numStartPerClass) {
					doRandExplore = true;
					doInitialSearch = true;
				}
			}

		}
		//		else if (processedCount > 9000) {
		//			doRandExplore = true;
		//		}

		learnerCxt = new DenseMatrix64F(ltask.getContext().toArray2());
		QueueArm.setCurrentContext(learnerCxt);

		// Update the world based on the results of processing the previous item
		processPrevBlock();


		if (doArmScan && initialArmList.size() == 0) {
			System.out.println("Done scanning. Items sent to learner: " + itemsSeenRewards.size());
			doArmScan = false;
		}

		if (doRandExplore) {
			while (item == null || itemsSeenRewards.containsKey(item.getItemId())) {
				item = itemList.remove(0);
			}
			initialItems.add(item.getItemId());
		}
		else if (doArmScan) {
			while (item == null && initialArmList.size() > 0) {
				armPulled = initialArmList.remove(initialArmList.size() - 1);

				if (armPulled != null) {
					item = armPulled.getItem();
					Integer itemId = item.getItemId();

					if (itemsSeenRewards.containsKey(itemId)) {
						if (ExperimentParameters.check("updateReward", "true")) {
							Double updatedReward = ltask.updateReward(itemId);
							if (updatedReward != null) {
								itemsSeenContexts.put(itemId, learnerCxt);
								itemsSeenRewards.put(itemId, updatedReward);
							}
						}

						updateArmList(itemsSeenContexts.get(itemId), itemsSeenRewards.get(itemId), itemId);		
						cacheHits++;

						//						if (armPulled != null && !armPulled.isEmpty() && !item.getLabel().equals("other")) {
						//							initialArmList.add(armPulled);
						//						}

						armPulled = null;
						item = null;
					}
				}
			}

			if (item == null) item = itemList.remove(0); 
		}
		else {

			int m = 0;

			if (armHeap == null || !MatrixFeatures.isIdentical(learnerCxt, lastCxt, 0.01)) {
				armHeap = new PriorityQueue<QueueArm>(armList);
				returnArmToHeap(armPulled);
				//System.out.println("RESORT**********");
			}

			while (item == null && armList.size() > 0) {
				m++;
				// Get the arm with the highest estimated reward
				armPulled = pickArm(learnerCxt);

				//lastEstimate = armPulled.getContextEstimate(learnerCxt);

				item = armPulled.getItem();
				Integer itemId = item.getItemId();

				//								String s = "Using arm: "  + data.getFeatureName(armPulled.getId()) + " " + armPulled.getId() +
				//										" Size: " + armPulled.size() + " Estimate: " + armPulled.getEstimate() + 
				//										//" Actual: " + armPulled.getActualReward() + " Rank: " + armPulled.getActualRank() +
				//										" Arm Count: " + armList.size() + " Arm Heap: " + armHeap.size() + " Old Item: " + itemsSeenRewards.containsKey(itemId);
				//								System.out.println(s);

				if (itemsSeenRewards.containsKey(itemId)) { // || initialItems.contains(item.getItemId())) {
					if (ExperimentParameters.check("updateReward", "true")) {
						Double updatedReward = ltask.updateReward(itemId);
						if (updatedReward != null) {
							itemsSeenContexts.put(itemId, learnerCxt);
							itemsSeenRewards.put(itemId, updatedReward);
						}
					}

					if (itemsSeenRewards.containsKey(itemId)) {
						//updateArmList(itemsSeenContexts.get(itemId), itemsSeenRewards.get(itemId), itemId);		
						updateArmList(learnerCxt, itemsSeenRewards.get(itemId), itemId);		
					}

					cacheHits++;
					item = null;
					//returnArmToHeap(armPulled);
				}

			}
		}


		if (item != null) {
			lastRecordId = item.getItemId();

			lastCxt = learnerCxt;

			processedCount++;

			item.setDataSource(data);

			nextBlock = new ItemDataBlock(item); 
			// System.out.println("Num pulled: " + armsPulled.size());
		}
		else {
			System.out.println("ITEM WAS NULL! items: " + itemList.size() + ", arms: " + armList.size());
			//loadArms();
		}
		//System.out.println("Cache hits: " + cacheHits);
		return nextBlock;
	}

	private QueueArm pickArm(DenseMatrix64F context) {
		QueueArm pickedArm = null;

		if (armPulled == null || armPulled.isEmpty()) {
			while (pickedArm == null || pickedArm.isEmpty()) {
				pickedArm = armHeap.poll();
			}
		}
		// The compare to is > 0 because the arms are sorted in reverse order in the heap.
		else if (armHeap.size() > 0 && armPulled.compareTo(armHeap.peek()) > 0) {
			pickedArm = armHeap.poll();
			returnArmToHeap(armPulled);
		}
		else {
			pickedArm = armPulled;
		}

		return pickedArm;
	}

	private void updateArmList(DenseMatrix64F context, double reward, int itemId) {
		armPulled.updateArmContextReward(context, reward, itemId);		

		if (armPulled.isEmpty()) {
			armList.remove(armPulled);
			armPulled = null;
		}

	}

	private void updateAllArmList(DenseMatrix64F context, double reward, int itemId) {
		Set<QueueArm> toRemove = new HashSet<QueueArm>();

		if (!initialItems.contains(itemId)) {
			armPulled.updateArmContextReward(context, reward, itemId);	

			for (QueueArm arm : armHeap) {
				if (arm.nextItemMatches(itemId)) {
					arm.updateArmContextReward(context, reward, itemId, true);
					toRemove.add(arm);
				}
			}
			if (toRemove.size() > 0) {
				armHeap.removeAll(toRemove);
				armHeap.addAll(toRemove);
			}
		}

		if (armPulled.isEmpty()) {
			armList.remove(armPulled);
			armPulled = null;
		}

	}

	private void returnArmToHeap(QueueArm arm) {
		if (arm != null && !arm.isEmpty()) {
			armHeap.offer(arm);
		}
	}

	public void processPrevBlock() {
		// Do this if we've already sent a block to process. We start building the
		// policy's training set after the learning task has processed a record.
		ProcessingResults procResults = ltask.getProcessingResults();

		if (procResults != null) {
			Integer itemId = procResults.getItemId();
			Item item = itemCache.get(itemId);

			//			if (doArmScan && armPulled != null && !armPulled.isEmpty() && !procResults.getLabel().equals("other")) {
			//				initialArmList.add(armPulled);
			//			}

			double reward = procResults.getReward();///procResults.getUdfTime();

			//System.out.println("Estimate: " + lastEstimate + ", Reward: " + reward);

			if (ExperimentParameters.get("timeSensitive") != null) {
				double normFactor = (Math.log(item.size()) - minItemSize)/(maxItemSize - minItemSize);
				reward = reward/normFactor;
			}

			//System.out.println(reward + " : " + procResults.getUdfTime());

			currentAccuracy = procResults.getAccuracy();

			if (lastRecordId > 0) {
				if (armPulled != null) {	
					updateArmList(learnerCxt, reward, itemId);			
				}

				//returnArmToHeap(armPulled);

				itemsSeenRewards.put(itemId, reward);
				//	itemsSeenContexts.put(itemId, lastCxt);
			}
		}
	}

	private void loadArms() {
		itemList = new ArrayList<Item>(data.getItems(testSize));
		int contextSize = lastCxt.numRows;

		HashMap<Integer, QueueArm> arms = new HashMap<Integer, QueueArm>();			

		HashMap<Integer, Float> features = null;

		for (Item item : itemList) {
			int itemId = item.getItemId();

			if (excludeIds.contains(itemId)) continue;

			features = data.getFeaturesForDoc(itemId);
			QueueArm arm = null;
			for (Map.Entry<Integer, Float> entry : features.entrySet()) {
				int featureId = entry.getKey();

				arm = arms.get(featureId);

				if (arm == null) {
					arm = new QueueArm(featureId, data, itemCache);
					arm.initMatrices(contextSize);
					arm.getContextEstimate(lastCxt);

					arms.put(featureId, arm);
				}

				arm.addItem(item);
			}
		}

		armList = new LinkedList<QueueArm>(arms.values());


		armsAreLoaded = true;
	}

	public double[] getUncertaintyError(int itemId) {
		double[] output = {};
		return output;
	}

	private void pruneArms() {
		// Prune all arms of size 1
		System.out.println("Pruning arms");
		Set<QueueArm> remove = new HashSet<QueueArm>();
		Set<Set<Item>> checkHash = new HashSet<Set<Item>>();

		QueueArm globalArm = null; // we need to find the global arm and save it

		if (ExperimentParameters.check("randomizedIndex", "true")) { 
			randomizeArms();
		}

		for (QueueArm arm : armList) {
			// Global arm has ID = 0.
			if (arm.getId() == 0) globalArm = arm;

			Set<Item> itemSet = arm.getItemSet();

			if (arm.getId() != 0 && !ExperimentParameters.check("indexType", "cluster") && 
					!ExperimentParameters.check("indexType", "lsh") && !ExperimentParameters.check("indexType", "heatMapClusters") && 
					(itemSet.size() <= 3*0.0001*itemList.size() || itemSet.size() > itemList.size() * 0.005 || checkHash.contains(itemSet))) {
				//if (arm.getId() != 0 && (itemSet.size() <= itemList.size() * 0.002 || checkHash.contains(itemSet))) {
				remove.add(arm);
				arm.isPruned();
			}
			else {
				checkHash.add(itemSet);

				if (ExperimentParameters.check("armSort", "itemSizeAscending")) {
					Collections.sort(arm.getItemList(), new Comparator<Item>(){
						public int compare(Item item1, Item item2) {
							return Double.compare(item1.size(), item2.size());
						}
					});
				} else if (ExperimentParameters.check("armSort", "itemSizeDescending")) {
					Collections.sort(arm.getItemList(), new Comparator<Item>(){
						public int compare(Item item1, Item item2) {
							return Double.compare(item2.size(), item1.size());
						}
					});
				} else if (ExperimentParameters.check("armSort", "randomKey")) {
					// Sorted by Random ID
					Collections.sort(arm.getItemList(), new Comparator<Item>(){
						public int compare(Item item1, Item item2) {
							return Double.compare(item1.getSortId(), item2.getSortId());
						}
					});
				} else {
					Collections.shuffle(arm.getItemList());
				}

			}
		}

		System.out.println("Arms: " + armList.size() + " Removing: " + remove.size());

		List<Integer> removedIds = new ArrayList<Integer>();
		for (QueueArm arm : remove) {
			armList.remove(arm);
			removedIds.add(arm.getId());
		}


		// Sort the arms randomly (but using stable random IDs)
		Collections.sort(armList, new Comparator<QueueArm>(){
			public int compare(QueueArm a1, QueueArm a2) {
				return Double.compare(a1.getRandomSortId(), a2.getRandomSortId());
			}
		});



		if (ExperimentParameters.get("armBudget") != null) {
			ArrayList<QueueArm> armsToPrune = new ArrayList<QueueArm>(armList);

			int armBudget = Integer.valueOf(ExperimentParameters.get("armBudget"));
			String pruneType = null;
			if (ExperimentParameters.get("pruneType") != null) {
				pruneType = ExperimentParameters.get("pruneType");
			}
			else {
				pruneType = "random";
			}

			if (pruneType.equals("bySize")) {			
				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a1.size(), a2.size());
					}
				});
			}
			else if (pruneType.equals("tfidf")) {
				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a2.getMeanTfIdf(testSize), a1.getMeanTfIdf(testSize));
					}
				});
			}			
			else if (pruneType.equals("keepBigItems")) {
				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a2.getMeanItemSize(), a1.getMeanItemSize());
					}
				});
			}			
			else if (pruneType.equals("keepSmallItems")) {
				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a1.getMeanItemSize(), a2.getMeanItemSize());
					}
				});
			}
			else if (pruneType.equals("homogeneity")) {
				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a1.getHomogeneityScore(), a2.getHomogeneityScore());
					}
				});
			}
			else if (pruneType.equals("featureCount")) {
				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a1.getItemFeatureCount(), a2.getItemFeatureCount());
					}
				});
			}

			else if (pruneType.equals("minorityItemCount")) {
				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a2.getMinorityItemCount(), a1.getItemFeatureCount());
					}
				});
			}

			HashMap<Integer, QueueArm> newArmMap = new HashMap<Integer, QueueArm>();
			newArmMap.put(0, globalArm);

			while (newArmMap.size() < armBudget && newArmMap.size() < armList.size()) {
				QueueArm arm = armsToPrune.remove(0);
				newArmMap.put(arm.getId(), arm);
			}

			for (QueueArm prunedArm : armsToPrune) {
				prunedArm.setIsPruned();
			}

			armList = new LinkedList<QueueArm>(newArmMap.values());

			// Need to a final shuffle to ensure randomness
			Collections.sort(armList, new Comparator<QueueArm>(){
				public int compare(QueueArm a1, QueueArm a2) {
					return Double.compare(a1.getRandomSortId(), a2.getRandomSortId());
				}
			});
			System.out.println("Arm count: " + armList.size());
		}

		initialArmList = new ArrayList<QueueArm>(armList);



		int mislabelArmCount = Integer.valueOf(ExperimentParameters.get("mislabelArmCount", "0"));
		Set<Integer> mislabelIds = new HashSet<Integer>();	

		if (mislabelArmCount > 0) {
			// try to load the mislabeled items from a file, so we can be consistent across tests
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
				System.out.println("Loaded mislabeled IDs from file: " + inFileName);
			// if we can't find the file, create it
			} catch (FileNotFoundException e) {
				System.out.println("Mislabel ID file not found. Creating it.");

				int usedArmsCount = 0;
				int i = -1;
				while (usedArmsCount < mislabelArmCount && i < armList.size()) {
					i++;
					if (armList.get(i).getId() == 0) {
						continue;
					}

					for (Item item : armList.get(i).getItemList()) {
						if (ExperimentParameters.check("noisyFeatures", "true")) item.setShouldRandomizeFeatures(true);
						mislabelIds.add(item.getItemId());
					}
					usedArmsCount++;
				}
				System.out.println("Mislabeled item count: " + mislabelIds.size());

				if (mislabelIds.size() > 0) {
					String outFileName = "mislabel_" + ExperimentParameters.get("corpusSize") + 
							"_" + mislabelArmCount + "_" + ExperimentParameters.get("testRunNumber") + ".txt";
					try {
						PrintWriter writer = new PrintWriter(outFileName);
						writer.print(Joiner.on("\n").join(mislabelIds));
						writer.close();
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

				}
			}
			
			if (mislabelIds.size() > 0) {
				Labeler labeler = data.getLabeler();
				((WikiRegressionLabeler) labeler).setMislabelSet(mislabelIds, 1.0);
			}
		}

		if (ExperimentParameters.check("doSubset", "true")) {
			globalArm.clearArm();
			Set<Item> subsetItems = new HashSet<Item>();
			for (QueueArm a : armList) {
				subsetItems.addAll(a.getItemList());
			}
			for (Item item : subsetItems) {
				globalArm.addItem(item);
			}
			armList = new ArrayList<QueueArm>();
			armList.add(globalArm);
			System.out.println("Num items: " + subsetItems.size());
		}
		System.out.println("Num arms: " + armList.size());
		// dumpArmData();
		// dumpItemData();
		// dumpArmContents();
	}

	public void randomizeArms() {
		System.out.println("Shuffling item list.");
		List<Item> randList = new ArrayList<Item>(itemList);
		Collections.shuffle(randList);
		System.out.println("Building map");
		Map<Integer, Item> randMap = new HashMap<Integer, Item>();

		for (int i = 0; i < itemList.size(); i++) {
			randMap.put(itemList.get(i).getItemId(), randList.get(i));
		}

		for (QueueArm arm : armList) {
			if (arm.getId() == 0) continue;

			int num = arm.size();
			List<Item> armItems = new ArrayList<Item>(arm.getItemList());
			arm.clearArm();

			for (Item item : armItems) {
				arm.addItem(randMap.get(item.getItemId()));
			}
		}
	}

	public void dumpArmData() {
		try {
			PrintWriter writer = new PrintWriter("arm_data.txt", "UTF-8");
			writer.println("ID\tName\tMean Item Size\tNum. Items\tMinority Items\tMinority Pct.\tFirst Minority Item");
			for (QueueArm arm : armList) {
				int firstMinorItem = -1;
				if (arm.getMinorityItemCount() > 0) {
					for (Item item : arm.getItemList()) {
						firstMinorItem++;
						if (!item.getLabel().equals("other")) break;
					}
				}
				writer.println(arm.getId() + "\t" + data.getFeatureName(arm.getId()) + "\t" + arm.getMeanItemSize() + "\t" + arm.size() + "\t" + arm.getMinorityItemCount() + "\t" + arm.getActualReward() + "\t" + firstMinorItem);
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

	public void dumpArmContents() {
		try {
			PrintWriter writer = new PrintWriter("arm_contents.txt", "UTF-8");
			Collections.sort(armList);
			for (QueueArm arm : armList) {
				writer.print(arm.getEstimate() + ",");
				for (Item item : arm.getItemList()) {
					writer.print(item.getSortId()+",");
				}
				writer.print("\n");
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

	public void dumpItemData() {
		try {
			PrintWriter writer = new PrintWriter("item_data.txt", "UTF-8");
			writer.println("ID\tSize\tLabel");
			for (Item item : itemList) {
				writer.println(item.getItemId() + "\t" + item.size() + "\t" + item.getLabel());
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		// armPullIds is armId for each pull
		// armPulled is <pull #> -> arms influenced by this pull

		//		for (Integer i = 0; i < armPullIds.size(); i++) {
		//			ArrayList<String> line = new ArrayList<String>();
		//			int pullId = armPullIds.get(i);
		//			for (Integer j = 0; j < i; j++) {
		//				if (armsPulled.get(j).contains(pullId))
		//					line.add((i+1) + "\t" + (j+1) + "\t1");
		//			}
		//			pw.println(Joiner.on("\n").join(line));
		//		}
		//pw.close();
	}


	@Override
	public double[] getOutput() {
		// TODO Auto-generated method stub
		//	return getUncertaintyError(lastRecordId);
		double[] vals =  {(double) cacheHits, 0.0, 0.0, 0.0, 0.0, 0.0};
		return vals;
	}

}
