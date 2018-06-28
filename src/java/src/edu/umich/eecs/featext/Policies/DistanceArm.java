package edu.umich.eecs.featext.Policies;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.harness.ExperimentParameters;

public class DistanceArm implements Comparable<DistanceArm> {
	int id;
	DataSource data;

	DenseMatrix64F centroid;
	DenseMatrix64F centroidVariance;
	DenseMatrix64F distanceArgs;
	DenseMatrix64F resultMatrix;
	
	int locationCount = 0;
	
	Double distance = Double.POSITIVE_INFINITY;
	
	int pullCount = 0;

	Set<Integer> items;

	int itemsAvail = 0;

	private boolean empty; 
	private boolean pruned;

	List<Item> itemList = new ArrayList<Item>();
	Set<Item> itemSet;

	private int randomSortId;
	private static DenseMatrix64F currentContext;


	static Comparator<DistanceArm> distComparator = new Comparator<DistanceArm>() {
		public int compare(DistanceArm obj1, DistanceArm obj2) {
			return obj1.getDistance().compareTo(obj2.getDistance());
		}
	};

	public DistanceArm(int id, DataSource data) {
		this.id = id;
		this.data = data;
		//sortedByActual.add(this);
		
		int runNum = 1;
		if (ExperimentParameters.get("testRunNumber") != null) {
			runNum = Integer.valueOf(ExperimentParameters.get("testRunNumber"));
		}
		Random rand = new Random();
		rand.setSeed(id * runNum);
		randomSortId = rand.nextInt();
	}

	public int getId() {
		return id;
	}

	public void initMatrices(int contextSize) {
		// contextSize includes the plane's intercept, so subtract that out 
		// for the centroid and variance.
		centroid = new DenseMatrix64F(contextSize-1, 1);
		centroidVariance = new DenseMatrix64F(contextSize-1, 1);
		distanceArgs = new DenseMatrix64F(contextSize, 1);
		distanceArgs.set(0, 1.0);
		
		resultMatrix = new DenseMatrix64F(1,1);
		
		if (currentContext == null) {
			currentContext = new DenseMatrix64F(contextSize, 1);
		}
	}

	public void update(Integer itemId, DenseMatrix64F location) {
		CommonOps.scale(locationCount, centroid);
		CommonOps.addEquals(centroid, location);
		locationCount++;
		CommonOps.divide(locationCount, centroid);
		CommonOps.insert(centroid, distanceArgs, 1, 0);
		
		// compute variance, too
	}

	public double computePlaneDistance(DenseMatrix64F planeCoeffs) {
		// intercept will be first coefficient
		CommonOps.multTransA(planeCoeffs, distanceArgs, resultMatrix);
		
		distance = Math.abs(resultMatrix.get(0));
		return distance;
	}
	
	public Double getDistance() {
		return distance;
	}
	
	public Double getSortValue() {
		if (empty) return Double.NEGATIVE_INFINITY;
		else return (double) computePlaneDistance(currentContext);
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
	
	public void addItem(Item item) {
		Item.setCurrentFeature(id);
		itemList.add(item);
		empty = false;
	}

	public int getPullCount() {
		return pullCount;
	}

	public List<Item> getItemList() {
		return itemList;
	}

	public Set<Item> getItemSet() {
		if (itemSet == null) {
			itemSet = new HashSet<Item>(itemList);
		}
		return itemSet;
	}

	public static void setCurrentContext(DenseMatrix64F context) {
		currentContext = context;
	}


	public int size() {
		return itemList.size();
	}


	public String toString() {
		return "DistanceArm(" + Arrays.toString(centroid.getData()) + ", " + size() + " items)";
	}


	@Override
	// This should allow items to be sorted by value increasing
	public int compareTo(DistanceArm otherArm){
		return this.getSortValue().compareTo(otherArm.getSortValue());
	}

	public void removeItem(Item item) {
		itemList.remove(item);
		itemSet.remove(item);
	}
}
