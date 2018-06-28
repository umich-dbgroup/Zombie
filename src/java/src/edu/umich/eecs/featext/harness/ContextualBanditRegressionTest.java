package edu.umich.eecs.featext.harness;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.SynthRegressionDataSource;
import edu.umich.eecs.featext.DataSources.WikipediaDataSource;
import edu.umich.eecs.featext.DataSources.WikipediaRegressionDataSource;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Policies.SerialContextualBanditPolicy;
import edu.umich.eecs.featext.Tasks.BasicClassifierTask;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.LinearRegressionTask;
import edu.umich.eecs.featext.Tasks.LogRegClassifierTask;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.RegressionKeyWordsUDF;
import edu.umich.eecs.featext.UDFs.SynthRegressionUDF;
import edu.umich.eecs.featext.UDFs.UDF;


public class ContextualBanditRegressionTest {
	double aoc = 0.0;
	int numTrials = 0;

	void runTest(int iterations, int numToProcess, String label, boolean reweight) {
		for (int i = 1; i <= iterations; i++) {
			ArrayList<UDF> udfList = new ArrayList<UDF>();

			//udfList.add(SynthRegressionUDF.createUDF("keywords"));
			udfList.add(RegressionKeyWordsUDF.createUDF("keywords"));

			ExperimentParameters.set("testRunNumber", "" + i);
			ExperimentParameters.set("corpusSize", "" + numToProcess);

			//DataSource data = new SynthRegressionDataSource();
			DataSource data = new WikipediaRegressionDataSource();
			
			Policy policy = SerialContextualBanditPolicy.createPolicy("count-" + numToProcess + "-" + i);

			String taskDesc = "learningTask";
			if (reweight) taskDesc = "reweight";
			LearningTask learningTask = new LinearRegressionTask(taskDesc, data, udfList);

			Path fname = new Path("test_data/wikipedia/wiki.seq");

			System.out.println("------------ ContextualBanditRegressionTest ------------");
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
		String context = "constant";
		String reward = "cosineDistance";
		
		String goodArmCount = "20";

		ExperimentParameters.set("numItemsToProcess", "10000");
		
		ExperimentParameters.set("contextType", context);
		ExperimentParameters.set("rewardType", reward);

		ExperimentParameters.set("goodArmCount", goodArmCount);
		
		ExperimentParameters.set("alpha", "3.0");
		ExperimentParameters.set("numClasses", "6");

		ExperimentParameters.set("indexType", "cluster");
		ExperimentParameters.set("clusterFile","clusters_500.txt");
		
		ExperimentParameters.set("mislabelArmCount", "25");
//		ExperimentParameters.set("noisyFeatures", "true");
//		ExperimentParameters.set("noiseType", "random");
	
		ContextualBanditRegressionTest contextBandit = new ContextualBanditRegressionTest();
		contextBandit.runTest(10, 100000, "regLen_noise_cos", true);
	}
}
