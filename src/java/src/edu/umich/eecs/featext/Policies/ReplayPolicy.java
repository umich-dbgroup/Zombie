/**
 * ReplayPolicy: A policy that will simply vend items taken from a text file
 * listing one ID per line.
 */
package edu.umich.eecs.featext.Policies;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import java.util.Collections;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.UDFs.UDF;

/**
 * @author Mike Anderson
 *
 */
public class ReplayPolicy implements Policy {
	DataSource data;
	LearningTask ltask;
	UDF udf;
	Path fname;
	
	String replayFile;
	
	private ArrayList<Integer> remainingIds;
	private ArrayList<Integer> allIds;
	private ArrayList<Integer> usedIds;
	
	public static final String ID_STRING = "replay";

	/**
	 * <code>createPolicy</code> returns an instance of the
	 * policy with the given label.  Right now there's only
	 * one.
	 */
	public static Policy createPolicy(String fileName) {
		return new ReplayPolicy(fileName);
	}

	public ReplayPolicy(String fileName) {
		replayFile = fileName;
	}

	@Override
	public void init(DataSource data, LearningTask ltask, Path fname) {
		this.data = data;
		this.ltask = ltask;
		this.fname = fname;

        allIds = new ArrayList<Integer>();

		List<String> lines;
		try {
			System.out.println("Replaying: " + replayFile);
			lines = FileUtils.readLines(new File(replayFile));
			for (String line : lines) {
				allIds.add(Integer.parseInt(line.trim()));
			}
            //Collections.shuffle(allIds);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		remainingIds = new ArrayList<Integer>(allIds);
		usedIds = new ArrayList<Integer>();
		
		System.out.println("Number of items: " + allIds.size());
	}

	@Override
	public DataBlock getNextDataBlock() {
		DataBlock nextBlock = null;

		if (this.remainingIds.size() == 0) {
			this.remainingIds = new ArrayList<Integer>(this.allIds);
		}

		if (this.remainingIds.size() > 0) {
			Integer nextId = this.remainingIds.get(0);

			this.remainingIds.remove(nextId);
			this.allIds.remove(nextId);

			nextBlock = data.getBlockForItem(nextId);
			
			this.usedIds.add(nextId);
		}		
		
		return nextBlock;
	}

	public double[] getUncertaintyError(int itemId) {
		double score = 0.0;

		double uncert = ltask.getItemUncertainty(itemId);

		// Find the rank of this item: how many items have a higher uncertainty?
		// We ideally will be picking the item with the highest uncertainty.
		int rank = 0;
		double maxUncert = 0.0;
		int maxId = 0;
		HashMap<Integer, Double> uncerts = ltask.getUncertainties(allIds);

		for (int id : allIds) {
			double evalItemUncert = uncerts.get(id);

			if (evalItemUncert > maxUncert) {
				maxUncert = evalItemUncert;
				maxId = id;
			}

			if (evalItemUncert > uncert) {
				rank++;
			}
		}

		score = (double)(rank)/(allIds.size()); // items should be already removed from allItems	
		double lastEstimate = 0.0; // No estimates for baseline case
		double[] output = {uncert, rank, score, lastEstimate, maxUncert, maxId};

		return output;
	}
	
	@Override
	public double[] getOutput() {
		// TODO Auto-generated method stub
//		return getUncertaintyError(lastRecordId);
		double[] vals =  {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		return vals;
	}
}
