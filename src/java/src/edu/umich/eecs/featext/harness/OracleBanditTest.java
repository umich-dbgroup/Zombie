package edu.umich.eecs.featext.harness;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.DataSources.WikiLabeler;
import edu.umich.eecs.featext.DataSources.WikipediaDataSource;
import edu.umich.eecs.featext.Policies.ContextualBanditPolicy;
import edu.umich.eecs.featext.Policies.OracleBanditPolicy;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.LogRegClassifierTask;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.UDF;


public class OracleBanditTest {
	void runTest(int iterations, int numToProcess, String label, boolean reweight) {
		for (int i = 1; i <= iterations; i++) {
			ArrayList<UDF> udfList = new ArrayList<UDF>();

			udfList.add(KeyWordsUDF.createUDF("keywords"));

			DataSource data = new WikipediaDataSource();
			
			Policy policy = OracleBanditPolicy.createPolicy("count-" + numToProcess + "-" + i);

            String taskDesc = "learningTask";
            if (reweight) taskDesc = "reweight";
            LearningTask learningTask = new LogRegClassifierTask(taskDesc, data, udfList);

			Path fname = new Path("test_data/wikipedia/wiki.seq");

            System.out.println("------------ OracleBanditTest ------------");
			System.out.println("Trial " + i +": " + learningTask.getDescription() + 
					" " + data.getDescription() + 
					" " + udfList.get(0).getDescription());

			double minPerf = -1;
			long maxRuntime = 30 * 1000;

			TestHarness harness = new TestHarness(data, udfList, policy, learningTask, fname);
			try {
				String testName = label + "_oracle_" + numToProcess +"." + i; 
				harness.test(minPerf, maxRuntime, numToProcess, testName);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	public static void main(String[] args) {
		ExperimentParameters.set("contextType","featureVariance");
		ExperimentParameters.set("rewardType", "wronglyClassified");
		
		OracleBanditTest contextBandit = new OracleBanditTest();
		contextBandit.runTest(10, 10000, "2class", true);
	}
}
