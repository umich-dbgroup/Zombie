/**
 * 
 */
package edu.umich.eecs.featext.Policies;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.DataSources.WikiLabeler;
import edu.umich.eecs.featext.DataSources.WikiRegressionLabeler;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.harness.ExperimentParameters;

/**
 * @author Mike Anderson
 *
 */
public class BaselinePolicy implements Policy {
	DataSource data;
	LearningTask ltask;
	Path fname;

	public static final String ID_STRING = "synth_baseline";

	String taskDesc;
	private List<Item> items;

	static int testSize;

	/**
	 * <code>createPolicy</code> returns an instance of the
	 * policy with the given label.  Right now there's only
	 * one.
	 */
	public static Policy createPolicy(String policyId) {
		String[] args = policyId.split("-");
		if (args.length == 3 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
		} else if (args.length == 2 && args[0].equals("count")) {
			testSize = Integer.parseInt(args[1]);
		}
		else {
			testSize = 20000;
		}
		return new BaselinePolicy(policyId);
	}

	public BaselinePolicy(String policyId) {}

	@Override
	public void init(DataSource data, LearningTask ltask, Path fname) {
		this.data = data;
		this.ltask = ltask;
		this.fname = fname;

		loadItems();
	}

	@Override
	public DataBlock getNextDataBlock() {
		DataBlock nextBlock = null;

		if (items.size() > 0) {
			Item nextItem = items.remove(items.size() - 1);
			
			nextItem.setDataSource(data);

			nextBlock = new ItemDataBlock(nextItem);
		}

		return nextBlock;
	}

	public double[] getUncertaintyError(int itemId) {

		double[] output = {};

		return output;
	}

	private void loadItems() {
		items = data.getItems(testSize);
		Collections.shuffle(items);

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
		if (mislabelIds.size() > 0) ((WikiLabeler) labeler).setMislabelSet(mislabelIds, 1.0);
	}

	@Override
	public double[] getOutput() {
		double[] vals =  {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		return vals;
	}
}
