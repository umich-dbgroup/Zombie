/**
 * 
 */
package edu.umich.eecs.featext.Policies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.MatrixFeatures;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.ProcessingResults;
import edu.umich.eecs.featext.UDFs.UDF;
import edu.umich.eecs.featext.harness.ExperimentParameters;

/**
 * @author Mike Anderson
 *
 */
public class DistancePolicy implements Policy {
	private int testSize;
	DataSource data;
	LearningTask ltask;
	UDF udf;
	Path fname;

	private ArrayList<DistanceArm> emptyArms = new ArrayList<DistanceArm>();

	private List<Item> itemList;

	public static final String ID_STRING = "distance_policy";

	private Set<Integer> excludeIds;

	private int lastRecordId = 0;

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
	Map<Integer, DenseMatrix64F> itemsSeenLocations = new HashMap<Integer, DenseMatrix64F>();

	Set<Integer> initialItems = new HashSet<Integer>();

	DistanceArm armPulled;
	private List<DistanceArm> armList;
	private boolean doRandExplore;
	private boolean doInitialSearch = false;
	private int processedCount = 0;

	private Set<DistanceArm> armsPulled = new HashSet<DistanceArm>();
	private boolean doFullArmScan = false;
	private Integer firstId;

	/**
	 * <code>createPolicy</code> returns an instance of the
	 * policy with the given label.  Right now there's only
	 * one.
	 */
	public static Policy createPolicy(String policyId) {
		return new DistancePolicy(policyId);
	}

	public DistancePolicy(String policyId) {
		String[] args = policyId.split("-");
		if (args.length == 3 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
		} else if (args.length == 2 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
		}
		else {
			testSize = 2000;
		}	
		
		rand.setSeed(310);
	}

	@Override
	public void init(DataSource data, LearningTask ltask, Path fname) {
		this.data = data;
		this.ltask = ltask;
		this.fname = fname;

		excludeIds = ltask.getTestSet();

		lastCxt = new DenseMatrix64F(ltask.getContext().toArray2());

		// TODO: maybe move this elsewhere
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

		learnerCxt = new DenseMatrix64F(ltask.getContext().toArray2());
		DistanceArm.setCurrentContext(learnerCxt);

		// Update the world based on the results of processing the previous item
		processPrevBlock();

		Item item = null;
		armPulled = null;

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

			if (!doInitialSearch) 				
				Collections.sort(armList);

		}
		else if (processedCount > 9000) {
			doRandExplore = true;
		}

		if (doRandExplore) {
			while (item == null || itemsSeenRewards.containsKey(item.getItemId())) {
				item = itemList.remove(0);
			}
			initialItems.add(item.getItemId());
		}
		else if (doFullArmScan) {
			
			while (item == null && armList.size() > 0) {
				armPulled = armList.remove(0);
				if (firstId == null) {
					firstId = armPulled.getId();
				}
				else if (firstId == armPulled.getId()) {
					doInitialSearch = false;
				}
				armsPulled.add(armPulled);

				item = armPulled.getItem();
				Integer itemId = item.getItemId();

				if (itemsSeenRewards.containsKey(itemId)) {
					updateArmList(itemsSeenLocations.get(itemId), itemId);		
					item = null;
				}

//								String s = "Using arm: "  + data.getFeatureName(armPulled.getId()) + " " + armPulled.getId() +
//											" Size: " + armPulled.size() + " Estimate: " + armPulled.getEstimate() + 
//											//" Actual: " + armPulled.getActualReward() + " Rank: " + armPulled.getActualRank() +
//											" Arm Count: " + armList.size() + " Old Item: " + itemsSeenRewards.containsKey(itemId);
//								System.out.println(s);
				
				((LinkedList<DistanceArm>) armList).addLast(armPulled);	

			}
			
			if (!doInitialSearch) {			
				Collections.sort(armList);
				System.out.println("Done initial search");
			}
		}
		else {
			int m = 0;

			if (!MatrixFeatures.isIdentical(learnerCxt, lastCxt, 0.01)) {	
				Collections.sort(armList);
				//System.out.println("RESORT**********");
			}

			while (item == null && armList.size() > 0) {
				m++;
				// Get the arm with the highest estimated reward					
				armPulled = pickArm(learnerCxt);
				armsPulled.add(armPulled);


				item = armPulled.getItem();
				Integer itemId = item.getItemId();



				//				String s = "Using arm: "  + data.getFeatureName(armPulled.getId()) + " " + armPulled.getId() +
				//							" Size: " + armPulled.size() + " Estimate: " + armPulled.getEstimate() + 
				//							//" Actual: " + armPulled.getActualReward() + " Rank: " + armPulled.getActualRank() +
				//							" Arm Count: " + armList.size() + " Old Item: " + itemsSeenRewards.containsKey(itemId);
				//				System.out.println(s);

				if (itemsSeenLocations.containsKey(itemId)) {
					updateArmList(itemsSeenLocations.get(itemId),  itemId);		
					item = null;
				}
				
			}
		}
		//		System.out.println("Arms pulled for next item: " + m);
		//		System.out.println("Context: " + learnerCxt);


		if (item != null) {
			//itemList.remove(item);
			//usedItems.add(item.getItemId());
			lastRecordId = item.getItemId();

			lastCxt = learnerCxt;

			processedCount++;

			nextBlock = new ItemDataBlock(item); 
			// System.out.println("Num pulled: " + armsPulled.size());
		}
		else {
			System.out.println("ITEM WAS NULL! items: " + itemList.size() + ", arms: " + armList.size());
			//loadArms();
		}

		return nextBlock;
	}

	private DistanceArm pickArm(DenseMatrix64F context) {
		DistanceArm pickedArm = null;
		// Get the arm with the smallest distance
		double minDist = Double.POSITIVE_INFINITY;

		List<DistanceArm> armPool = new ArrayList<DistanceArm>();
		
		for (DistanceArm a : armList) {
			if (a.isEmpty()) continue; // TODO: should we have empty arms here?

			// We are going to add all the arms with the same maximum estimate
			// to a pool and then select one at random. Maybe more efficient
			// to do reservoir sampling here, but this is easier for now.
			if (minDist == Double.POSITIVE_INFINITY) {
				minDist = a.getDistance();
			}
			else if (a.getDistance() != minDist) {
				break;
			}
			armPool.add(a);
		}

		pickedArm = armPool.get(rand.nextInt(armPool.size()));

		return pickedArm;
	}

	private void updateArmList(DenseMatrix64F itemLoc, int itemId) {
		armPulled.update(itemId, itemLoc);

		armList.remove(armPulled);

		if (armPulled.isEmpty()) {
			emptyArms.add(armPulled);
		}
		else {			
			double dist = armPulled.computePlaneDistance(learnerCxt);
			boolean didInsert = false;
			ListIterator<DistanceArm> it = ((LinkedList<DistanceArm>) armList).listIterator();
			while (it.hasNext()) {
				double nextDist = it.next().computePlaneDistance(learnerCxt);
				if (nextDist >= dist) {
					it.previous();
					it.add(armPulled);
					didInsert = true;
					break;
				}
			}
			if (!didInsert) armList.add(armPulled);
		}
	}


	public void processPrevBlock() {
		// Do this if we've already sent a block to process. We start building the
		// policy's training set after the learning task has processed a record.
		if (lastRecordId > 0) {
			ProcessingResults procResults = ltask.getProcessingResults();

			Integer itemId = procResults.getItemId();
			
			DenseMatrix64F location = new DenseMatrix64F(procResults.getFeatures().length, 1);
			location.set(procResults.getFeatures().length, 1, false, procResults.getFeatures());
			
			if (armPulled != null) {	
				updateArmList(location, itemId);			
			}

			itemsSeenLocations.put(itemId, location);
		}
	}

	private void loadArms() {
		itemList = new LinkedList<Item>(data.getItems(testSize));

		int contextSize = lastCxt.numRows;

		HashMap<Integer, DistanceArm> arms = new HashMap<Integer, DistanceArm>();

		for (Item item : itemList) {
			int itemId = item.getItemId();

			if (excludeIds.contains(itemId)) continue;

			HashMap<Integer, Float> features = data.getFeaturesForDoc(itemId);
			for (Map.Entry<Integer, Float> entry : features.entrySet()) {
				int featureId = entry.getKey();
				float featureVal = entry.getValue();

				DistanceArm arm = arms.get(featureId);

				if (arm == null) {
					arm = new DistanceArm(featureId, data);
					arm.initMatrices(contextSize);
					arms.put(featureId, arm);
				}

				item.setValue(featureId, featureVal);
				arm.addItem(item);
			}
		}

		armList = new LinkedList<DistanceArm>(arms.values());

		armsAreLoaded = true;
	}

	public double[] getUncertaintyError(int itemId) {
		double[] output = {};
		return output;
	}

	private void pruneArms() {
		// Prune all arms of size 1
		Set<DistanceArm> remove = new HashSet<DistanceArm>();
		Set<Set<Item>> checkHash = new HashSet<Set<Item>>();

		DistanceArm globalArm = null; // we need to find the global arm and save it

		for (DistanceArm arm : armList) {
			// Global arm will be the biggest one.
			if (globalArm == null || arm.size() > globalArm.size()) globalArm = arm;

			Set<Item> itemSet = arm.getItemSet();
			if (itemSet.size() <= 1 || checkHash.contains(itemSet)) {
				remove.add(arm);
				arm.isPruned();
			}
			else {
				checkHash.add(itemSet);

				if (ExperimentParameters.get("armSort") != null && ExperimentParameters.get("armSort").equals("itemSizeAscending")) {
					Collections.sort(arm.getItemList(), new Comparator<Item>(){
						public int compare(Item item1, Item item2) {
							return Double.compare(item1.size(), item2.size());
						}
					});
				} else if (ExperimentParameters.get("armSort") != null && ExperimentParameters.get("armSort").equals("itemSizeDescending")) {
					Collections.sort(arm.getItemList(), new Comparator<Item>(){
						public int compare(Item item1, Item item2) {
							return Double.compare(item2.size(), item1.size());
						}
					});
				}
				else {
					//Collections.shuffle(arm.getItemList());
					// Sorted by Random ID
					Collections.sort(arm.getItemList(), new Comparator<Item>(){
						public int compare(Item item1, Item item2) {
							return Double.compare(item1.getSortId(), item2.getSortId());
						}
					});
				}

			}
		}

		System.out.println("Arms: " + armList.size() + " Removing: " + remove.size());
		for (DistanceArm arm : remove) {
			armList.remove(arm);
		}

		// Sort the arms randomly (but using stable random IDs)
		Collections.sort(armList, new Comparator<DistanceArm>(){
			public int compare(DistanceArm a1, DistanceArm a2) {
				return Double.compare(a1.getRandomSortId(), a2.getRandomSortId());
			}
		});



		if (ExperimentParameters.get("armBudget") != null) {
			ArrayList<DistanceArm> armsToPrune = new ArrayList<DistanceArm>(armList);

			int armBudget = Integer.valueOf(ExperimentParameters.get("armBudget"));
			String pruneType = null;
			if (ExperimentParameters.get("pruneType") != null) {
				pruneType = ExperimentParameters.get("pruneType");
			}
			else {
				pruneType = "random";
			}

			if (pruneType.equals("bySize")) {			
				Collections.sort(armsToPrune, new Comparator<DistanceArm>(){
					public int compare(DistanceArm a1, DistanceArm a2) {
						return Double.compare(a1.size(), a2.size());
					}
				});
			}
			

			HashMap<Integer, DistanceArm> newArmMap = new HashMap<Integer, DistanceArm>();
			newArmMap.put(0, globalArm);

			while (newArmMap.size() < armBudget && newArmMap.size() < armList.size()) {
				DistanceArm arm = armsToPrune.remove(0);
				newArmMap.put(arm.getId(), arm);
			}

			for (DistanceArm prunedArm : armsToPrune) {
				prunedArm.setIsPruned();
			}

			armList = new LinkedList<DistanceArm>(newArmMap.values());

			// Need to a final shuffle to ensure randomness
			Collections.sort(armList, new Comparator<DistanceArm>(){
				public int compare(DistanceArm a1, DistanceArm a2) {
					return Double.compare(a1.getRandomSortId(), a2.getRandomSortId());
				}
			});
			System.out.println("Arm count: " + armList.size());
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
		double[] vals =  {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		return vals;
	}

}
