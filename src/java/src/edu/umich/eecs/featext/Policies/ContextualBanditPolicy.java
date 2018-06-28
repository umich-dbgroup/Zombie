/**
 * 
 */
package edu.umich.eecs.featext.Policies;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.ejml.data.DenseMatrix64F;
import org.jblas.DoubleMatrix;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.ProcessingResults;
import edu.umich.eecs.featext.UDFs.UDF;
import edu.umich.eecs.featext.harness.ExperimentParameters;

/**
 * @author Mike Anderson
 *
 */
public class ContextualBanditPolicy implements Policy {
	private static int testSize;
	DataSource data;
	LearningTask ltask;
	UDF udf;
	Path fname;

	private HashMap<Integer, QueueArm> arms;
	private ArrayList<QueueArm> emptyArms = new ArrayList<QueueArm>();

	public static ItemCache itemCache = new ItemCache();

	private ArrayList<Integer> allBlocks;

	private ArrayList<Item> itemList;
	private HashSet<Integer> usedItems;
	private HashSet<Integer> usedBlocks;

	public static final String ID_STRING = "contextual_bandit";

	private Set<Integer> excludeIds;

	private int lastRecordId = 0;

	private boolean armsAreLoaded = false;

	private int itemsInQueues = 0;

	private DoubleMatrix learnerCxt;

	PrintWriter pw;

	static Random rand = new Random();

	// Here are some parameters
	int initialSeedCount = 10; // Number of random documents to examine before 
	// we start querying arms.

	int numStartPerClass = 1; // Number of items needed per class to end random exploration	

	double epsilon = 0.0;//
	private Labeler labeler;

	static int startBlock;

	static int runId = 0;
	
	List<Double> rankings = new ArrayList<Double>();
	private List<Double> bestActuals = new ArrayList<Double>();

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
			runId = Integer.parseInt(args[2]);
		} else if (args.length == 2 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
			startBlock = rand.nextInt(1200); // number of wiki blocks
		}
		else {
			testSize = 2000;
			startBlock = rand.nextInt(1200); // number of wiki blocks
		}
		return new ContextualBanditPolicy(policyId);
	}

	public ContextualBanditPolicy(String policyId) {}

	@Override
	public void init(DataSource data, LearningTask ltask, Path fname) {
		this.data = data;
		this.ltask = ltask;
		this.fname = fname;

		excludeIds = ltask.getTestSet();

		List<Integer> blockIds = data.getBlockIds();

		//Collections.shuffle(blockIds);

		allBlocks = new ArrayList<Integer>(blockIds);

		itemList = new ArrayList<Item>();
		usedItems = new HashSet<Integer>();
		usedBlocks = new HashSet<Integer>();

		itemCache = new ItemCache();

		arms = new HashMap<Integer, QueueArm>();

		labeler = data.getLabeler();

		try {
			pw = new PrintWriter(new FileOutputStream("test_data/arm_ranking_" + ID_STRING + "_" + runId + ".txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public DataBlock getNextDataBlock() {
		DataBlock nextBlock = null;

		// Update the world based on the results of processing the previous item
		processPrevBlock();

		learnerCxt = ltask.getContext();
		DenseMatrix64F newContext = new DenseMatrix64F(ltask.getContext().toArray2());

		// TODO: this is very task-specific. Remove or generalize. 
		int[] labelCounts = ltask.getCounts();

		boolean doRandExplore = false;
		for (int cnt : labelCounts) {
			if (cnt < numStartPerClass) doRandExplore = true;
		}

		// Load the arms if we haven't yet
		// TODO: maybe move this elsewhere
		if (!armsAreLoaded) {
			int blockCount = (testSize/2300) + 2;
			System.out.print("Loading " + blockCount + " blocks:");
			for (int i = 0; i < blockCount; i++) {
				System.out.print(" " + (i+1));
				loadArms(findBestBlock());
			}
			System.out.print("\n");

			pruneArms();			
		}


		QueueArm arm = null;
		Item item = null;

		// TODO: need to terminate when we're all out of arms/blocks
		while (item == null) {
			// Get an item from the best non-empty arm.
			if (doRandExplore) {
				arm = arms.get(0);
				item = arm.getItem(usedItems, lastRecordId);
				if (labeler.getLabel(item.getItemId()).equals("other")) {
					if (rand.nextBoolean()) {
						arm.addItem(item);
						item = arm.getItem(usedItems, lastRecordId);
					}

				}
			} 
			else {
				while (item == null && arms.size() > 0) {
					// Get the arm with the highest estimated reward
					double maxEstimate = Double.NEGATIVE_INFINITY;
					for (QueueArm a : arms.values()) {
						
						if (a.isEmpty()) continue; // TODO: should we have empty arms here?
						
						double est = a.getContextEstimate(newContext);
						if (est > maxEstimate) {
							arm = a;
							maxEstimate = est;
						}
					}

					// Tiny chance that one might not get returned. If so, just pull
					// an item off the global arm (arm_0).
					// TODO: is this still necessary?
					if (arm == null || arm.isEmpty()) {
						System.out.println("no best arm. Empty: " + arm.isEmpty());
						arm = arms.get(0);
					}

					// Get an item from the arm.
					if (arm != null) {
						
						QueueArm best = QueueArm.getBestArmByActualReward();
						rankings.add(arm.getActualRank());
						bestActuals.add(best.getActualReward());
						
						item = arm.getItem(usedItems, lastRecordId);
					}
					// TODO: We shouldn't get here. Haven't seen it lately, can we remove this?
					else {
						System.out.println("Arm was NULL! Arms: " + arms.size() + ", Items: " + itemList.size());
						for (QueueArm tmpArm : arms.values()) {
							System.out.println("Arm: " + tmpArm.getId() + ", Size: " + tmpArm.size() + ", Est: " + tmpArm.getEstimate() + ", Empty: " + tmpArm.isEmpty());
						}
						item = itemList.get(0);
					}
				}

			}

			if (item != null) {
				itemList.remove(item);
				usedItems.add(item.getItemId());
				lastRecordId = item.getItemId();

				nextBlock = new ItemDataBlock(item); 

				//				if (arm != null) {              
				//					System.out.println("Using arm: " + arm.getId() + 
				//							" Estimate: " + arm.getEstimate() +
				//							" Context: " + learnerCxt + " Examined: " + arms.size() + " DocId: " + item.getItemId());
				//				}
				itemsInQueues--;

			}
			else {
				System.out.println("ITEM WAS NULL! items: " + itemList.size() + ", arms: " + arms.size());
				//loadArms(findBestBlock());
			}
		}

		return nextBlock;
	}

	private DataBlock findBestBlock() {
		processPrevBlock();

		DataBlock nextBlock = null;

		if (allBlocks.size() == 0) {
			return nextBlock; // null
		}
		else { 
			Integer nextId = this.allBlocks.get(startBlock);

			nextBlock = data.getBlockContaining(nextId);

			this.allBlocks.remove(nextId);
			this.usedBlocks.add(nextId);

			initialSeedCount--;
		}

		return nextBlock;
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
		for (int i = 0; i < rankings.size(); i++) {
			pw.println(rankings.get(i) + "," + bestActuals.get(i));
		}
		pw.close();
	}

	public void processPrevBlock() {
		// Do this if we've already sent a block to process. We start building the
		// policy's training set after the learning task has processed a record.
		if (lastRecordId > 0) {	
			ProcessingResults procResults = ltask.getProcessingResults();

			DenseMatrix64F newContext = new DenseMatrix64F(ltask.getContext().toArray2());

			Integer itemId = procResults.getItemId();
			double reward = procResults.getReward()/procResults.getUdfTime();

			HashMap<Integer, Float> features = data.getFeaturesForDoc(itemId); //itemCache.get(itemId).getFeatures();

			for (Integer featureId : features.keySet()) {
				QueueArm arm = arms.get(featureId);

				if (arm != null) {
					arm.updateArmContextReward(learnerCxt, reward, itemId);		

					if (arm.isEmpty()) {
						emptyArms.add(arm);
						arms.remove(arm.getId());
					}
					else {
						// Compute the context estimate here, since 
						// only the ones updated here need to be recomputed
						arm.getContextEstimate(newContext);
					}
				}

			}


			learnerCxt = ltask.getContext();;
		}
	}

	private void loadArms(DataBlock block) {
		IntWritable key = new IntWritable();
		Writable contents = new Text();

		int contextSize = ltask.getContext().length;
		
		long pos = block.getPosition();
		long oldPos = -1;

		while (block.next(key, contents)) {
			if (itemList.size() >= testSize) break;

			int itemId = key.get();

			if (excludeIds.contains(itemId)) continue;

			oldPos = pos;
			pos = block.getPosition();
			long offset = oldPos;
			long length = pos - oldPos;
			itemsInQueues++;

			Item item = itemCache.get(itemId);

			if (!item.isInitialized()) {
				Writable itemContents = new Text((Text) contents);
				item.initialize(itemContents, offset, length);
			}
			else {
				// We've seen this item. It's a duplicate for some reason.
				continue;
			}

			boolean loadedAnArm = false;

			HashMap<Integer, Float> features = data.getFeaturesForDoc(itemId);
			for (Map.Entry<Integer, Float> entry : features.entrySet()) {
				int featureId = entry.getKey();
				float featureVal = entry.getValue();

				QueueArm arm = arms.get(featureId);

				if (arm == null) {
					arm = new QueueArm(featureId, data, itemCache);
					arm.initMatrices(contextSize);
					arms.put(featureId, arm);
				}

				item.setValue(featureId, featureVal);
				arm.addItem(item);
				loadedAnArm = true;
			}

			if (loadedAnArm) {
				itemList.add(item);
			}
		}

		armsAreLoaded = true;
	}

	public double[] getUncertaintyError(int itemId) {
		double[] output = {};
		return output;
	}

	private void pruneArms() {
		// Prune all arms of size 1
		Set<Integer> remove = new HashSet<Integer>();
		Set<Set<Item>> checkHash = new HashSet<Set<Item>>();


		for (QueueArm arm : arms.values()) {
			Set<Item> itemSet = arm.getItemSet();
			if (itemSet.size() <= 1 || checkHash.contains(itemSet)) {
				remove.add(arm.getId());
			}
			else {
				checkHash.add(itemSet);

				if (ExperimentParameters.get("armSort") != null && ExperimentParameters.get("armSort").equals("itemSizeAscending")) {
					Collections.sort(arm.getItemList(), new Comparator<Item>(){
						public int compare(Item a1, Item a2) {
							return Double.compare(a1.size(), a2.size());
						}
					});
				} else if (ExperimentParameters.get("armSort") != null && ExperimentParameters.get("armSort").equals("itemSizeDescending")) {
					Collections.sort(arm.getItemList(), new Comparator<Item>(){
						public int compare(Item a1, Item a2) {
							return Double.compare(a2.size(), a1.size());
						}
					});
				}
				else {
					Collections.shuffle(arm.getItemList());
				}

			}
		}

		System.out.println("Arms: " + arms.size() + " Removing: " + remove.size());
		for (Integer id : remove) {
			arms.remove(id);
		}

//		PrintWriter apw;
//		try {
//			apw = new PrintWriter(new FileOutputStream("test_data/arm_values_" + ID_STRING + ".txt"));
//
//			for (QueueArm arm : arms.values()) {
//				apw.println(arm.getId() + "," + arm.size() + "," + arm.getMeanTfIdf(testSize));
//			}
//			apw.close();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		ArrayList<QueueArm> armsToPrune = new ArrayList<QueueArm>(arms.values());

		if (ExperimentParameters.get("armBudget") != null) {
			int armBudget = Integer.valueOf(ExperimentParameters.get("armBudget"));
			String pruneType = null;
			if (ExperimentParameters.get("pruneType") != null) {
				pruneType = ExperimentParameters.get("pruneType");
			}
			else {
				pruneType = "random";
			}

			if (pruneType.equals("bySize")) {
				Collections.shuffle(armsToPrune);

				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a1.size(), a2.size());
					}
				});
			}
			else if (pruneType.equals("tfidf")) {
				Collections.shuffle(armsToPrune);

				Collections.sort(armsToPrune, new Comparator<QueueArm>(){
					public int compare(QueueArm a1, QueueArm a2) {
						return Double.compare(a2.getMeanTfIdf(testSize), a1.getMeanTfIdf(testSize));
					}
				});
			}
			else {
				Collections.shuffle(armsToPrune); // do it randomly
			}

			HashMap<Integer, QueueArm> newArmMap = new HashMap<Integer, QueueArm>();
			newArmMap.put(0, arms.get(0));

			while (newArmMap.size() < armBudget && newArmMap.size() < arms.size()) {
				QueueArm arm = armsToPrune.remove(0);
				newArmMap.put(arm.getId(), arm);
			}

			arms = newArmMap;
			System.out.println("Arm count: " + arms.size());
		}
	}

	@Override
	public double[] getOutput() {
		// TODO Auto-generated method stub
		//	return getUncertaintyError(lastRecordId);
		double[] vals =  {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		return vals;
	}

}
