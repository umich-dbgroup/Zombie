/**
 *  OracleBanditPolicy: this bandit goes through all of its arms ahead of time
 *  to have prior knowledge of which arm is best to choose.
 */
package edu.umich.eecs.featext.Policies;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.jblas.DoubleMatrix;

import com.google.common.base.Joiner;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.WikiLabeler;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.UDFs.UDF;

/**
 * @author Mike Anderson
 *
 */
public class OracleBanditPolicy implements Policy {
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

	public static final String ID_STRING = "oracle_bandit";

	private Set<Integer> excludeIds;

	private int lastRecordId = 0;
	private double lastEstimate = 0.0;

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

	static int startBlock;
	
	
	private ArrayList<Integer> armPullIds = new ArrayList<Integer>();
	private int nextPullIdx = 0;
	private ArrayList<Set<Integer>> armsPulled = new ArrayList<Set<Integer>>();
	private HashMap<Integer, Set<Integer>> armInfluence = new HashMap<Integer, Set<Integer>>();
 
	
	// Oracle related stuff goes down here
	
	private WikiLabeler labeler = new WikiLabeler();
	private String[] labels = labeler.getLabels();
	private HashMap<Integer, ArrayList<Integer>> armLabelCounts = new HashMap<Integer, ArrayList<Integer>>();
	
	
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
			startBlock = rand.nextInt(1200); // number of wiki blocks
		}
		else {
			testSize = 2000;
			startBlock = rand.nextInt(1200); // number of wiki blocks
		}
		return new OracleBanditPolicy(policyId);
	}

	public OracleBanditPolicy(String policyId) {}

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

		try {
			pw = new PrintWriter(new FileOutputStream("test_data/arm_pulls_" + ID_STRING + ".txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public DataBlock getNextDataBlock() {
		DataBlock nextBlock = null;

		processPrevBlock();

		learnerCxt = ltask.getContext();
		int[] labelCounts = ltask.getCounts();

		boolean doRandExplore = false;
		for (int cnt : labelCounts) {
			if (cnt < numStartPerClass) doRandExplore = true;
		}

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
		//		else if (itemList.size() < 1) {
		//			synchronized(armList) {
		//				loadArms(findBestBlock());
		//			}
		//		}

		QueueArm arm = null;
		Item item = null;

		// TODO: need to terminate when we're all out of arms/blocks
		while (item == null) {
			// Get an item from the best non-empty arm.
			if (doRandExplore) {
				arm = arms.get(0);
				item = arm.getItem(usedItems, lastRecordId);
			} 
			else {
				while (item == null && arms.size() > 0) {
					double val = -1.0;
					ArrayList<Integer> toRemove = new ArrayList<Integer>();

					for (Entry<Integer, ArrayList<Integer>> entry : armLabelCounts.entrySet()) {
						Integer armId = entry.getKey();
						ArrayList<Integer> counts = entry.getValue();
						int total = 0;
						for (Integer cnt : counts) {
							total += cnt;
						}
						
						// Assume that we're looking for the one with the most
						// of label 0, which should be the minority label
						double pct0 = (double) counts.get(0)/total;
						QueueArm tmpArm = arms.get(armId);

						if (!tmpArm.isEmpty() && pct0 > val) {
							arm = tmpArm;
							val = pct0;
						}
						else if (tmpArm.isEmpty()) {
							toRemove.add(tmpArm.getId());
						}
					}
					
					item = arm.getItem(usedItems, lastRecordId);

					for (Integer i : toRemove) {
						arms.remove(i);
						armLabelCounts.remove(i);
					}
				}

			}

			if (item != null) {
				itemList.remove(item);
				usedItems.add(item.getItemId());
				lastRecordId = item.getItemId();

				nextBlock = new ItemDataBlock(item); // data.getBlockForOffset(item.getOffset(), item.getLength());

				if (arm != null) {
					arm.setMaxEstimate(arm.getEstimate());
					lastEstimate = arm.getEstimate();
              
//					System.out.println("Using arm: " + arm.getId() + 
//							" Estimate: " + arm.getEstimate() +
//							" Context: " + learnerCxt + " Examined: " + arms.size() + " DocId: " + item.getItemId());
					

					//if (arm.getId() != 0 && !armPullIds.contains(arm.getId())) 
					armPullIds.add(arm.getId());

					                 
				}
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
		else { // (!sawPositive || rand.nextDouble() <= epsilon || arms.size() == 0 || allBlocks.size() < 3) {
			Integer nextId = this.allBlocks.get(startBlock);

			nextBlock = data.getBlockContaining(nextId);

			this.allBlocks.remove(nextId);
			this.usedBlocks.add(nextId);

			initialSeedCount--;
		}
		//		else { // Get the arm with the highest UCB
		//			Integer nextId = null;
		//			Integer armId = null;
		//
		//			while (nextId == null) {
		//				Double maxUCB = 0.0;
		//
		//				List<QueueArm> blockArmList = new ArrayList<QueueArm>(arms.values());
		//				Collections.sort(blockArmList);
		//
		//				//				for (Map.Entry<Integer, QueueArm> entry : arms.entrySet()) {
		//				//					//if (entry.getValue().isEmpty()) continue;
		//				//
		//				//					if (entry.getValue().getEstimate() >= maxUCB) {
		//				//						armId = entry.getKey();
		//				//						maxUCB = entry.getValue().getEstimate();
		//				//					}
		//				//				}				
		//
		//				QueueArm arm = null;
		//				while (arm == null && blockArmList.size() > 0) {
		//					arm = blockArmList.get(0); //arms.get(armId);
		//					blockArmList.remove(0);
		//					
		//					lastQueueArm = arm;
		//					Set<Integer> featureDocs = arm.getDocs(usedBlocks, lastBlockId); 
		//
		//					if (featureDocs.size() == 0) {
		//						arm.setIsEmpty();
		//						arms.remove(arm.getId());
		//					}
		//					else {
		//						nextId = featureDocs.iterator().next();
		//					}
		//				}
		//				System.out.println("Getting block from arm: " + data.getFeatureName(arm.getId()) + 
		//						" Estimate: " + arm.getEstimate() +
		//						" UCB: " + arm.getUCB() + " Examined: " + arms.size() + " DocId: " + nextId);
		//			}
		//			nextBlock = data.getBlockContaining(nextId);
		//			lastBlockId = nextId;
		//			this.allBlocks.remove(nextId);
		//			this.usedBlocks.add(nextId);
		//		}

		return nextBlock;
	}

	public void close() {
		// armPullIds is armId for each pull
		// armPulled is <pull #> -> arms influenced by this pull
		
		for (Integer i = 0; i < armPullIds.size(); i++) {
			ArrayList<String> line = new ArrayList<String>();
			int pullId = armPullIds.get(i);
			for (Integer j = 0; j < i; j++) {
				if (armsPulled.get(j).contains(pullId))
					line.add((i+1) + "\t" + (j+1) + "\t1");
			}
			pw.println(Joiner.on("\n").join(line));
		}
		
//		for (Integer i = 0; i < armPullIds.size(); i++) {
//			ArrayList<String> line = new ArrayList<String>();
//			int pullId = armPullIds.get(i);
//			for (Integer j = 0; j < armPullIds.size(); j++) {
//				if (armInfluence.get(pullId).contains(j))
//					line.add((i+1) + "\t" + (j+1) + "\t1");
//			}
//			pw.println(Joiner.on("\n").join(line));
//		}
		
		pw.close();
	}

	public void processPrevBlock() {
		// Do this if we've already sent a block to process. We start building the
		// policy's training set after the learning task has processed a record.
		if (lastRecordId > 0) {	
			Map<Integer, Double> rewards = ltask.getAllItems();
			DoubleMatrix newContext = ltask.getContext();

			for (Map.Entry<Integer, Double> rewardEntry : rewards.entrySet()) {
				Integer itemId = rewardEntry.getKey();
				double reward = rewardEntry.getValue();

				HashMap<Integer, Float> features = itemCache.get(itemId).getFeatures();

				Set<Integer> pulls = new HashSet<Integer>();
				
				for (Integer featureId : features.keySet()) {
					QueueArm arm = arms.get(featureId);
					

					
					if (arm != null) {
						arm.updateArmContextReward(learnerCxt, reward, itemId);		
						
						if (reward > 0)
							pulls.add(featureId);
						
						if (arm.isEmpty()) {
							emptyArms.add(arm);
							arms.remove(arm.getId());
							armLabelCounts.remove(arm.getId());
						}
						else {
							// Compute the context estimate here, since 
							// only the ones updated here need to be recomputed
							arm.getContextEstimate(newContext);
						}
					}
					
				}
				armsPulled.add(pulls);

			}

			learnerCxt = newContext;
		}
	}

	private void loadArms(DataBlock block) {
		IntWritable key = new IntWritable();
		Writable contents = new Text();

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

			String itemLabel = labeler.getLabel(itemId);
			int labelIdx = -1;
			for (int i = 0; i < labels.length; i++) {
				if (labels[i].equals(itemLabel)) {
					labelIdx = i;
				}
			}
			
			boolean loadedAnArm = false;

			HashMap<Integer, Float> features = data.getFeaturesForDoc(itemId);
			for (Map.Entry<Integer, Float> entry : features.entrySet()) {
				int featureId = entry.getKey();
				float featureVal = entry.getValue();

				QueueArm arm = arms.get(featureId);

				if (arm == null) {
					arm = new QueueArm(featureId, data, itemCache);
					arms.put(featureId, arm);
					
					// Initialize the oracular arm label counts
					armLabelCounts.put(featureId, new ArrayList<Integer>());
					for (int i = 0; i < labels.length; i++) {
						armLabelCounts.get(featureId).add(0);
					}
				}

				armLabelCounts.get(featureId).set(labelIdx, armLabelCounts.get(featureId).get(labelIdx) + 1);
				
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
				
				Collections.shuffle(arm.getItemList());
			}
		}
		
		System.out.println("Arms: " + arms.size() + " Removing: " + remove.size());
		for (Integer id : remove) {
			arms.remove(id);
			armLabelCounts.remove(id);
		}
	}
	
	@Override
	public double[] getOutput() {
		// TODO Auto-generated method stub
		//	return getUncertaintyError(lastRecordId);
		double[] vals =  {0.0, 0.0, 0.0, lastEstimate, 0.0, 0.0};
		return vals;
	}

}
