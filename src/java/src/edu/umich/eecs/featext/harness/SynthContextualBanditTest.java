package edu.umich.eecs.featext.harness;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.SyntheticDataSource;
import edu.umich.eecs.featext.DataSources.WikipediaDataSource;
import edu.umich.eecs.featext.Policies.DistancePolicy;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Policies.SerialContextualBanditPolicy;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.LogRegClassifierTask;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.SyntheticUDF;
import edu.umich.eecs.featext.UDFs.UDF;


public class SynthContextualBanditTest {
	void runTest(int iterations, int numToProcess, String label, boolean reweight) {
		for (int i = 1; i <= iterations; i++) {
			ArrayList<UDF> udfList = new ArrayList<UDF>();
			udfList.add(SyntheticUDF.createUDF("keywords"));

			double rarity = ExperimentParameters.get("rarity", 0.005); // default 0.005
			int numClasses = ExperimentParameters.get("numClasses", 2); // default 2

			DataSource data = new SyntheticDataSource(numToProcess, numClasses, rarity);
			Policy policy = new SerialContextualBanditPolicy("count-" + numToProcess + "-" + ExperimentParameters.get("armBudget"));

			String taskDesc = "learningTask";
			if (reweight) taskDesc = "reweight";
			LearningTask learningTask = new LogRegClassifierTask(taskDesc, data, udfList);

			Path fname = new Path("test_data/wikipedia/wiki.seq");

			System.out.println("------------ SynthContextualBanditTest ------------");
			System.out.println("Trial " + i +": " + learningTask.getDescription() + 
					" " + data.getDescription() + 
					" " + udfList.get(0).getDescription());

			double minPerf = -1;
			long maxRuntime = 30 * 1000;

			TestHarness harness = new TestHarness(data, udfList, policy, learningTask, fname);
			try {
				String testName = label + "_context_" + numToProcess +"." + i; 
				harness.test(minPerf, maxRuntime, numToProcess, testName);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// ((SynthContextualBanditPolicy) policy).close();
		}
	}

	public static void main(String[] args) {
				ExperimentParameters.set("contextType","decisionBoundary");
				ExperimentParameters.set("rewardType", "decisionBoundary");
				ExperimentParameters.set("timeSensitive", "true");
		String[] budgets = {"10000"}; //{"10000", "20000", "30000", "40000", "50000", "60000", "70000", "80000", "90000", "100000"};

		for (String budget : budgets) {
			ExperimentParameters.set("armBudget", budget);
			ExperimentParameters.set("pruneType", "keepBigItems");
			ExperimentParameters.set("rarity", "0.005");
			SynthContextualBanditTest contextBandit = new SynthContextualBanditTest();
			contextBandit.runTest(5 , 10000, "02-28_featqual1", true);
			ExperimentParameters.reset();
		}
	}
}
