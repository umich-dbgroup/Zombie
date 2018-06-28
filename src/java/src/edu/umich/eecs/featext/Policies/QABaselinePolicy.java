/**
 * 
 */
package edu.umich.eecs.featext.Policies;

import java.util.ArrayList;
import java.util.Collections;
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

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.QADataSource;
import edu.umich.eecs.featext.DataSources.Question;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.UDFs.UDF;

/**
 * @author Mike Anderson
 *
 */
public class QABaselinePolicy implements Policy {
	DataSource data;
	LearningTask ltask;
	UDF udf;
	Path fname;
	private ArrayList<Integer> remainingIds;
	private ArrayList<Integer> usedIds;
	private ArrayList<Integer> allBlocks;

	private ArrayList<Integer> allItems;
	private HashSet<Integer> usedItems;
	private HashSet<Integer> usedBlocks;

	public static final String ID_STRING = "baseline";

	private boolean armsAreLoaded = false;

	public static ItemCache itemCache = new ItemCache();


	private Set<Integer> excludeIds;

	public static final int COLLECTION_MODE = 1492;
	public static final int VEND_MODE = 1776;

	String taskDesc;
	private int lastRecordId;
	private ArrayList<Item> items;

	static int testSize;
	private static int startBlock;
	static Random rand = new Random();
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
			
			//startBlock = 517;
		} else if (args.length == 2 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
			startBlock = rand.nextInt(1200); // number of wiki blocks
		}
		else {
			testSize = 2000;
			startBlock = rand.nextInt(1200); // number of wiki blocks
		}
		return new QABaselinePolicy(policyId);
	}

	public QABaselinePolicy(String policyId) {}

	@Override
	public void init(DataSource data, LearningTask ltask, Path fname) {
		this.data = data;
		this.ltask = ltask;
		this.fname = fname;

		excludeIds = ltask.getTestSet();

		//List<Integer> pageIds = WikiIndexer.getPageIds();
		List<Integer> blockIds = data.getBlockIds();

		//Collections.shuffle(blockIds);

		allBlocks = new ArrayList<Integer>(blockIds);
		usedBlocks = new HashSet<Integer>();
		allItems = new ArrayList<Integer>();
		items = new ArrayList<Item>();


		// We are using about 10 blocks worth for experiments
		//pageIds = new ArrayList<Integer>(pageIds.subList(0, 23000));

		usedIds = new ArrayList<Integer>();

	}

	@Override
	public DataBlock getNextDataBlock() {
		DataBlock nextBlock = null;
		
		if (!armsAreLoaded) {
			System.out.println("here");

			int startQuestionId = 1;
			loadArms(startQuestionId);			
			
			items = new ArrayList<Item>(items.subList(0, testSize));
		}

		Item bestItem = null;

		Collections.shuffle(items);

		bestItem = items.get(0);

		if (bestItem != null) {
			items.remove(bestItem);
			lastRecordId = bestItem.getItemId();

			nextBlock = new ItemDataBlock(bestItem);
		}

		return nextBlock;
	}

	public double[] getUncertaintyError(int itemId) {

		double[] output = {};

		return output;
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
		}

		return nextBlock;
	}

	private void loadArms(int startId) {
		ArrayList<Question> questions = ((QADataSource) data).getQuestions();

		for (int itemId = startId; itemId < questions.size(); itemId++) {
			
			if (allItems.size() >= testSize) continue;			
			if (excludeIds.contains(itemId)) continue;

			HashMap<Integer, Float> features = data.getFeaturesForDoc(itemId);
			int numNeg = 0;
			for (Map.Entry<Integer, Float> entry : features.entrySet()) {
				int featureId = entry.getKey();
				if (featureId <= 0) numNeg++;
			}

			//if (features.size() > numNeg) {
				Item item = ((QADataSource) data).getItem(itemId);

				allItems.add(itemId);
				items.add(item);
			//}
		}
		armsAreLoaded = true;
	}

	@Override
	public double[] getOutput() {
		// TODO Auto-generated method stub
		//		return getUncertaintyError(lastRecordId);
		double[] vals =  {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		return vals;
	}
}
