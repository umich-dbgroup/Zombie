package edu.umich.eecs.featext.harness;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.WikipediaDataSource;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Policies.SerialContextualBanditPolicy;
import edu.umich.eecs.featext.Tasks.BasicClassifierTask;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.LogRegClassifierTask;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.UDF;


public class ContextualBanditTest {
	double aoc = 0.0;
	int numTrials = 0;
	
	void runTest(int iterations, int numToProcess, String label, boolean reweight) {
		for (int i = 1; i <= iterations; i++) {
			ArrayList<UDF> udfList = new ArrayList<UDF>();

			udfList.add(KeyWordsUDF.createUDF("keywords"));

			ExperimentParameters.set("testRunNumber", "" + i);
			ExperimentParameters.set("corpusSize", "" + numToProcess);
			
			DataSource data = new WikipediaDataSource();
			
			Policy policy = SerialContextualBanditPolicy.createPolicy("count-" + numToProcess + "-" + i);

            String taskDesc = "learningTask";
            if (reweight) taskDesc = "reweight";
            LearningTask learningTask = new LogRegClassifierTask(taskDesc, data, udfList);

			Path fname = new Path("test_data/wikipedia/wiki.seq");

            System.out.println("------------ ContextualBanditTest ------------");
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

			aoc += harness.getAoc();
			numTrials++;
			
            ((SerialContextualBanditPolicy) policy).close();
            learningTask.close();
		}
	}
	
	public double getAoc() {
		return aoc/numTrials;
	}
	
	public static void main(String[] args) throws FileNotFoundException {
//		String context = "decisionBoundary";
//		String reward = "weightedDecisionBoundary";
		String context = "constant";
		String reward = "booleanLabelCounts";
		
		ExperimentParameters.set("contextType", context);
		ExperimentParameters.set("rewardType", reward);
		
		//ExperimentParameters.set("updateReward", "true");
		
		//ExperimentParameters.set("armSetSort", "2ndByItem");
		//ExperimentParameters.set("armSort", "randomKey");
		//ExperimentParameters.set("randomizedIndex", "true");
		//ExperimentParameters.set("timeSensitive", "true");
		//ExperimentParameters.set("splitArmsBySize", "true");
//		ArrayList<String> alphas = new ArrayList<String>();
//		alphas.add("0.001");
//		
//		for (double a = 0; a < 1.01; a += 0.05) {
//			alphas.add(String.format("%.3f", a));
//		}
//
//		PrintWriter pw = new PrintWriter(new FileOutputStream("test_data/output/alphas2_" + context + "_" + reward + ".txt"));
//		
//		for (String alpha : alphas) {
//			ExperimentParameters.set("armBudget", budget);
//			ExperimentParameters.set("pruneType", "keepBigItems");
			ExperimentParameters.set("mislabelArmCount", "100");
			ExperimentParameters.set("alpha", "0.6");
			ExperimentParameters.set("rarity", "0.005");
			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(10, 10000, "mislabelTest_err100_classbal", true);
//			pw.println(alpha + "\t" + contextBandit.getAoc()/10000);
//			pw.flush();
//		}
//		
//		pw.close();
	}
}
