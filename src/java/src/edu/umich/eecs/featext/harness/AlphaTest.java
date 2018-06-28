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


public class AlphaTest {
	double aoc = 0.0;
	int numTrials = 0;

	void runTest(int iterations, int numToProcess, String label, boolean reweight) {
		for (int i = 1; i <= iterations; i++) {
			ArrayList<UDF> udfList = new ArrayList<UDF>();

			udfList.add(KeyWordsUDF.createUDF("keywords"));

			ExperimentParameters.set("testRunNumber", "" + i);

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
		String context = "constant";
		String reward = args[0]; //"booleanLabelCounts";
		String numClasses = args[1];

		System.out.println(context+"-"+reward);

		ExperimentParameters.set("contextType", context);
		ExperimentParameters.set("rewardType", reward);

//		ExperimentParameters.set("mislabelArmCount", "100");
//		ExperimentParameters.set("noisyFeatures", "true");

		ExperimentParameters.set("rarity", "0.005");

		ExperimentParameters.set("indexType", "cluster");
		ExperimentParameters.set("clusterFile", "clusters_500.txt");

		ExperimentParameters.set("numClasses", numClasses);
        ExperimentParameters.set("numItemsToProcess", "50000");


		if (reward.equals("wronglyClassified")) ExperimentParameters.set("updateReward", "true");

		//ExperimentParameters.set("armSetSort", "2ndByItem");
		//ExperimentParameters.set("armSort", "randomKey");
		//ExperimentParameters.set("randomizedIndex", "true");
		//ExperimentParameters.set("timeSensitive", "true");
		//ExperimentParameters.set("splitArmsBySize", "true");
		ArrayList<String> alphas = new ArrayList<String>();
		alphas.add("2.1");
		alphas.add("2.2");
		alphas.add("2.3");
		alphas.add("2.4");
		alphas.add("2.5");
		alphas.add("2.6");
		alphas.add("2.7");
		alphas.add("2.8");
		alphas.add("2.9");
		alphas.add("3.0");

		for (double a = 0.1; a < 2.01; a += 0.1) {
			alphas.add(String.format("%.3f", a));
		}
		alphas.add("0.01");
		Collections.sort(alphas);

		PrintWriter pw = new PrintWriter(new FileOutputStream("test_data/output/alphas_cluster_500_" + numClasses + "_" + reward + ".txt"));

		for (String alpha : alphas) {
			ExperimentParameters.set("alpha", alpha);
			AlphaTest contextBandit = new AlphaTest();
			contextBandit.runTest(10, 1000000, "alpha_search_"+numClasses+"_"+reward, true);
			pw.println(alpha + "\t" + contextBandit.getAoc()/50000);
			pw.flush();
		}

		pw.close();
	}
}
