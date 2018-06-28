package edu.umich.eecs.featext.Policies;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.ejml.data.DenseMatrix64F;
import org.ejml.data.ReshapeMatrix64F;
import org.ejml.ops.CommonOps;
import org.ejml.ops.MatrixFeatures;
import org.jblas.DoubleMatrix;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.harness.ExperimentParameters;

public class QueueArm implements Comparable<QueueArm> {
	int id;
	DataSource data;

	private static double maxEstimate = 0.0;

	static double c = 0.01; // constant from Good-UCB algorithm
	double rewardTotal = 0.0; // total reward from this expert
	int itemCount = 10; // items from this expert, initialized with a smoothing value
	static int totalCount = 0; // number of items seen by the whole system
	static double totalCountTerm;
	static int blockCount = 0;

	int pullCount = 0;
	int maxSize = 0;

	Set<Integer> items;

	int itemsAvail = 0;

	double estimate = 0;
	double ucb = 0;
	private boolean empty; 
	private boolean pruned;

	private int largestFeatureCount = 1; // for normalizing

	Set<Integer> docs;

	HashMap<Integer, Boolean> removed = new HashMap<Integer, Boolean>();

	//PriorityQueue<Item> itemList = new PriorityQueue<Item>();
	List<Item> itemList = new ArrayList<Item>();
	Set<Item> itemSet;// = new HashSet<Item>();

	double alpha = 0.3;

	// Here are the contexts for contextual updates.
	// These are static so all arms share them.
	static DoubleMatrix armContext;
	static DoubleMatrix learnerContext;

	// These are from the algorithms in Li, et al. 2010
	DenseMatrix64F AMat;
	DoubleMatrix BMat;
	DenseMatrix64F bMat;
	private double lastReward;

	//private Set<Item> itemSet;
	private DenseMatrix64F eyeMat;
	private DenseMatrix64F theta;
	private DenseMatrix64F tempCol;
	private DenseMatrix64F Ainv;
	private DenseMatrix64F estimateMat;
	private DenseMatrix64F cxtMultiplied;
	private DenseMatrix64F workingContext;
	private DenseMatrix64F ctxSquare;

	private ItemCache itemCache;

	private double totalTf = 0.0;
	private double meanTfIdf = 0.0;

	private double totalItemSize = 0.0;
	private double meanItemSize = 0.0;
	private double homogeneityScore = 0.0;
	private double itemFeatureCount = 0.0;
	private int minorityItemCount = 0;

	private int labelTotal = 0;
	private double actualReward = 0.0;
	private DenseMatrix64F lastContext;
	private boolean updatedSinceEstimate = true; // start true to do initial calc
	private int randomSortId;
	private static DenseMatrix64F currentContext;

	private double randomMultiplier;

	public static List<QueueArm> sortedByActual = new ArrayList<QueueArm>();
	private static Comparator<QueueArm> actComparator = new Comparator<QueueArm>() {
		public int compare(QueueArm obj1, QueueArm obj2) {
			return obj2.getActualReward().compareTo(obj1.getActualReward());
		}
	};

	static Comparator<QueueArm> estComparator = new Comparator<QueueArm>() {
		public int compare(QueueArm obj1, QueueArm obj2) {
			return obj1.getEstimate().compareTo(obj2.getEstimate());
		}
	};

	public QueueArm(int id, DataSource data, ItemCache itemCache) {
		this.id = id;
		this.data = data;
		this.itemCache = itemCache;
		//sortedByActual.add(this);

		if (ExperimentParameters.get("alpha") != null) {
			alpha = Double.valueOf(ExperimentParameters.get("alpha"));
		}

		int runNum = 1;
		if (ExperimentParameters.get("testRunNumber") != null) {
			runNum = Integer.valueOf(ExperimentParameters.get("testRunNumber"));
		}
		Random rand = new Random();
		//rand.setSeed(id * runNum);
		randomSortId = rand.nextInt();
		randomMultiplier = 1.0;// + (rand.nextDouble() - 0.5)*0.0001;
	}

	public int getId() {
		return id;
	}

	public void update(Integer itemId, double reward, int totalProcessed, int blocksProcessed) {
		rewardTotal += reward;
		itemCount++;

		blockCount = blocksProcessed;

		estimate = rewardTotal/itemCount;

		if (itemId != null && itemId != 0 && itemList.contains(itemCache.get(itemId)))
			itemList.remove(itemCache.get(itemId));
	}

	public Double getUCB() {
		ucb = getEstimate();// + (totalCountTerm/Math.sqrt(itemCount));
		return ucb;
	}

	public Double getEstimate() {
		// Not sure why it is NaN.
		//		if (Double.isNaN(estimate)) {
		//			estimate = 0.0;
		//		}
		return estimate; //rewardTotal/(itemCount);
	}

	public Double getSortValue() {
		//return getContextEstimate(armContext);//getEstimate();
		if (empty) return Double.NEGATIVE_INFINITY;
		else return (double) getContextEstimate(currentContext); 
	}

	public Integer getRandomSortId() {
		return randomSortId;
	}

	public void setIsEmpty() {
		if (itemList.size() == 0)
			empty = true;
	}

	public boolean isEmpty() {
		if (!empty && itemList.size() == 0) setIsEmpty();
		return empty;
	}

	public void setIsPruned() {
		pruned = true;
	}

	public boolean isPruned() {
		return pruned;
	}

	public Item getItem() {
		Item nextItem = null;
		if (itemList.size() > 0) {
			Item.setCurrentFeature(id);
			nextItem = itemList.remove(itemList.size() - 1);

			// TODO: should we set it empty here if this was the last item?
		}
		else {
			setIsEmpty();
		}
		return nextItem;
	}

	public Item getItem(HashSet<Integer> usedIds, int lastRecordId) {
		Item nextItem = null;
		if (itemList.size() > 0) {
			Item.setCurrentFeature(id);
			nextItem = itemList.remove(0);

			// make sure it's not null and also hasn't already been used
			while (itemList.size() > 0 && (usedIds.contains(nextItem.getItemId()) || nextItem.getItemId() == lastRecordId)) {
				nextItem = itemList.remove(0);
			}

			if (nextItem == null || (usedIds.contains(nextItem.getItemId()) || nextItem.getItemId() == lastRecordId)) {
				setIsEmpty();
				nextItem = null;
			}
		}
		else {
			setIsEmpty();
		}
		return nextItem;
	}

	public void sortByDiversity() {
		ArrayList<Item> newList = new ArrayList<Item>();
		Set<Item> removed = new HashSet<Item>();
		Item lastItem = null;

		if (itemList.size() == 0) return;

		if (newList.size() == 0) {
			lastItem = itemList.remove(0);
			newList.add(lastItem);
		}

		for (int i = 0; i < itemList.size(); i++) {
			Set<Integer> lastFeatures = data.getFeaturesForDoc(lastItem.getItemId()).keySet();
			int maxDiff = Integer.MIN_VALUE;
			Item nextItem = null;

			for (Item item : itemList) {
				if (removed.contains(item)) continue;

				Set<Integer> curFeatures = data.getFeaturesForDoc(item.getItemId()).keySet();
				curFeatures.removeAll(lastFeatures);
				if (curFeatures.size() > maxDiff) {
					maxDiff = curFeatures.size();
					nextItem = item;
				}
			}

			removed.add(nextItem);
			newList.add(nextItem);
		}

		itemList = newList;
	}

	public Set<Integer> getDocs(HashSet<Integer> usedIds, Integer lastRecordId) {
		if (docs == null) {
			SortedSet<Entry<Integer, Float>> entries = entriesSortedByValues(data.getBlocksForFeature(id));
			Iterator<Entry<Integer, Float>> it = entries.iterator();
			docs = new TreeSet<Integer>();
			while (it.hasNext()) {
				docs.add(it.next().getKey());
			}
		}

		docs.removeAll(usedIds);
		docs.remove(lastRecordId);
		return docs;
	}

	public void addItem(Item item) {
		//	if (item.getValue(id) > largestFeatureCount) {
		//		largestFeatureCount = Math.round(item.getValue(id));
		//	}

		Item.setCurrentFeature(id);
		itemList.add(item);
		//itemSet.add(item);
		empty = false;

		if (item.getLabel() != null && !item.getLabel().equals("other")) {
			labelTotal++;
			minorityItemCount++;
		}

		totalTf += Math.log(item.getValue(id) + 1.0);
		totalItemSize += item.size();

		maxSize++;
	}

	public void clearArm() {
		itemList.clear();
		empty = true;
		labelTotal = 0;
		minorityItemCount = 0;
		totalTf = 0;
		totalItemSize = 0;
		maxSize = 0;
	}

	public int getPullCount() {
		return pullCount;
	}

	public int getMinorityItemCount() {
		return minorityItemCount;
	}

	public Double getActualReward() {
		if (isEmpty() || isPruned()) labelTotal = 0;
		return (double) labelTotal/(double) (size());
	}

	public static QueueArm getBestArmByActualReward() {
		Collections.sort(sortedByActual, actComparator);
		return sortedByActual.get(0);
	}

	public double getActualRank() {
		// assumed to be sorted
		int myrank = 1;
		int rank = 1;
		int n = 1;
		double curReward = Double.POSITIVE_INFINITY;
		for (QueueArm arm : sortedByActual) {
			if (arm.getActualReward() < curReward) {
				rank = n;
				curReward = arm.getActualReward();
			}


			//if (arm.getActualReward() > 0) System.out.println(n+": "+arm.getActualReward() + " - " + rank + " ids: " + this.getId() + ", " + arm.getId());

			if (arm.getId() == this.getId()) {
				myrank = rank;
				//System.out.println(n+": "+arm.getActualReward() + " - " + rank + " ids: " + this.getId() + ", " + arm.getId());
			}

			n++;
		}
		return (double) myrank/(double) rank;
	}

	public DoubleMatrix getArmContext() {
		DoubleMatrix context = new DoubleMatrix(2,1);
		context.put(0, pullCount);
		context.put(1, lastReward);
		return context;
	}

	public List<Item> getItemList() {
		return itemList;
	}

	public Set<Item> getItemSet() {
		return new HashSet<Item>(itemList);
	}

	public void initMatrices(int contextSize) {
		//contextSize += 1; // add intercept

		if (AMat == null) {
			AMat = CommonOps.identity(contextSize);
			bMat = new DenseMatrix64F(contextSize, 1);
			ctxSquare = CommonOps.identity(contextSize);

			lastContext = new DenseMatrix64F(contextSize, 1);
		}

		if (eyeMat == null) {
			eyeMat = CommonOps.identity(contextSize);
			theta = new DenseMatrix64F(contextSize, 1);
			tempCol = new DenseMatrix64F(contextSize, 1);
			Ainv = new DenseMatrix64F(contextSize, contextSize);
			estimateMat = new DenseMatrix64F(1,1);
		}

		if (cxtMultiplied == null) {
			cxtMultiplied = new DenseMatrix64F(contextSize, contextSize);
		}

		if (currentContext == null) {
			currentContext = new DenseMatrix64F(contextSize, 1);
		}

		workingContext = new DenseMatrix64F(contextSize, 1);
		//workingContext.set(0, 0, 1.0);
	}

	public static void setCurrentContext(DenseMatrix64F context) {
		currentContext = context;
	}

	public double getContextEstimate(DoubleMatrix armCxt) {
		DenseMatrix64F convertMatrix = new DenseMatrix64F(armCxt.toArray2());
		return getContextEstimate(convertMatrix);
	}

	public double getContextEstimate(DenseMatrix64F armCxt) {	
		// These calculations should only have to be done if either
		// 		1. The context has changed.
		//		2. This arm has has been updated with a new context/reward pair
		// Otherwise, we'll just return the last estimate.
		CommonOps.insert(armCxt, workingContext, 0, 0);

		if (!MatrixFeatures.isIdentical(workingContext, lastContext, 0.01) || updatedSinceEstimate) {	

			// We should only have to do this if there 
			// has been a new reward assigned to this arm.
			if (updatedSinceEstimate) {
				// First solve A * theta = b for theta
				CommonOps.invert(AMat, Ainv);
				CommonOps.mult(Ainv, bMat, theta);		
			}

			// Now for that theta and the newContext, find the new estimate
			CommonOps.multTransA(theta, workingContext, estimateMat);
			estimate = estimateMat.get(0);

			if (alpha > 0.0) {
				// a * sqrt(C' * Ainv * C)
				// reuse estimateMat
				CommonOps.mult(Ainv, workingContext, tempCol);
				CommonOps.multTransA(workingContext, tempCol, estimateMat);

				estimate += alpha * Math.sqrt(estimateMat.get(0));
			}

			estimate *= randomMultiplier;

			lastContext.set(workingContext);
			updatedSinceEstimate = false;
		}

		return estimate;
	}

	public void updateArmContextReward(DoubleMatrix armCxt, double reward, Integer itemId) {		
		DenseMatrix64F convertMatrix = new DenseMatrix64F(armCxt.toArray2());
		updateArmContextReward(convertMatrix, reward, itemId);
	}

	public void updateArmContextReward(DenseMatrix64F armCxt, double reward, Integer itemId) {		
		pullCount++;
		lastReward = reward;

		CommonOps.insert(armCxt, workingContext, 0, 0);
		CommonOps.multTransB(workingContext, workingContext, ctxSquare);		

		// A = A + C*C'
		CommonOps.addEquals(AMat, ctxSquare);

		// B = B + r*C
		CommonOps.addEquals(bMat, reward, workingContext);

		updatedSinceEstimate = true;
	}

	public void updateArmContextReward(DenseMatrix64F armCxt, double reward, Integer itemId, boolean doRemove) {		
		updateArmContextReward(armCxt,  reward,  itemId);	

		if (doRemove) {
			Item item = itemCache.get(itemId);
			itemList.remove(item);
		}
	}

	public static void incrementTotalCount() {
		totalCount++;
	}

	public int getNormalizeCount() {
		return largestFeatureCount;
	}

	// The most number of items this arm has held
	public int getMaxSize() {
		return maxSize;
	}

	public int size() {
		return itemList.size();
	}

	public double getMeanTfIdf(int corpusSize) {
		if (meanTfIdf == 0.0) {
			meanTfIdf = (totalTf/this.size()) * Math.log((double) corpusSize/this.size());
		}

		return meanTfIdf;
	}

	public double getMeanItemSize() {
		if (meanItemSize == 0.0) {
			meanItemSize = totalItemSize/this.size();
		}

		return meanItemSize;
	}

	public double getHomogeneityScore() {
		if (homogeneityScore == 0.0) {
			HashSet<Integer> features = new HashSet<Integer>();
			int featureCount = 0;

			for (Item item : itemList) {
				for (Integer featureId : data.getFeaturesForDoc(item.getItemId()).keySet()) {
					featureCount++;
					features.add(featureId);
				}
			}
			homogeneityScore = (double) featureCount/features.size();
		}

		return homogeneityScore;
	}

	// Count the number of unique feature in all the items in the arm
	public double getItemFeatureCount() {
		if (itemFeatureCount == 0.0) {			
			for (Item item : itemList) {
				itemFeatureCount += data.getFeaturesForDoc(item.getItemId()).size();
			}

			itemFeatureCount /= (double) itemList.size();
		}

		return itemFeatureCount;
	}

	public DenseMatrix64F[] getContextMatrices() {
		DenseMatrix64F[] out = new DenseMatrix64F[2];
		out[0] = AMat.copy();
		out[1] = bMat.copy();
		return out;
	}

	public void setContextMatrices(DenseMatrix64F[] mats) {
		this.AMat = mats[0];
		this.bMat = mats[1];
	}

	public void setMaxEstimate(double maxEst) {
		maxEstimate = maxEst;
		HashMap<Integer, Boolean> removed = new HashMap<Integer, Boolean>();
	}


	public String toString() {
		return "QueueArm(" + itemList.toString() + ")";
	}

	public static int computeHash(DoubleMatrix A, DoubleMatrix b, DoubleMatrix context) {
		int[] codes = {Arrays.hashCode(A.toArray()), Arrays.hashCode(b.toArray()), Arrays.hashCode(context.toArray())};


		//		return DigestUtils.md5Hex(A.toString().hashCode() + ""+ b.toString().hashCode() + ""+ context.toString().hashCode());
		return Arrays.hashCode(codes);
	}

	@Override
	// This should allow items to be sorted by value decreasing
	public int compareTo(QueueArm otherArm){
		boolean doSecondarySort = true;

		if (ExperimentParameters.check("armSetSort", "2ndByItem")) {
			// they have different values, just compare them
			if (!otherArm.getSortValue().equals(this.getSortValue())) {
				return otherArm.getSortValue().compareTo(this.getSortValue());
			}
			// otherwise, compare the items in the list. This should allow us
			// to process arms with the same item quickly in succession.
			else {
				int len = Math.min(otherArm.size(), this.size());
				for (int idx = 0; idx < len; ++idx) {
					int comp = Double.compare(this.getItemList().get(idx).getSortId(), otherArm.getItemList().get(idx).getSortId());
					if (comp != 0) return comp;
				}
				return otherArm.size() - this.size();
			}
		} else {
			return otherArm.getSortValue().compareTo(this.getSortValue());
		}
	}

	public static <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(Map<K, V> map) {
		SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<Map.Entry<K, V>>(
				new Comparator<Map.Entry<K, V>>() {
					@Override
					public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
						return e1.getValue().compareTo(e2.getValue());
					}
				});
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}

	public void removeItem(Item item) {
		itemList.remove(item);
		itemSet.remove(item);
	}

	public boolean nextItemMatches(int itemId) {
		return (itemList.size() > 0 && itemList.get(0).getItemId() == itemId);
	}
}
