package edu.umich.eecs.featext.harness;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.DataSources.WikiLabeler;
import edu.umich.eecs.featext.DataSources.WikipediaDataSource;
import edu.umich.eecs.featext.Policies.BaselinePolicy;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Tasks.BasicClassifierTask;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.LogRegClassifierTask;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.NERUDF;
import edu.umich.eecs.featext.UDFs.UDF;


public class BaselineNERTest {
	void runTest(int iterations, int numToProcess, String label, boolean reweight) {
		for (int i = 1; i <= iterations; i++) {
			ArrayList<UDF> udfList = new ArrayList<UDF>();

			udfList.add(NERUDF.createUDF("ner"));
			udfList.add(KeyWordsUDF.createUDF("keywords"));

			ExperimentParameters.set("testRunNumber", "" + i);
			ExperimentParameters.set("corpusSize", "" + numToProcess);
			
			DataSource data = new WikipediaDataSource();

			Policy policy = BaselinePolicy.createPolicy("count-" + numToProcess + "-" + i);

            String taskDesc = "learningTask";
            if (reweight) taskDesc = "reweight";
            LearningTask learningTask = new LogRegClassifierTask(taskDesc, data, udfList);

			Path fname = new Path("test_data/wikipedia/wiki.seq");

            System.out.println("------------ BaselineNERTest ------------");
			System.out.println("Trial " + i +": " + learningTask.getDescription() + 
					" " + data.getDescription() + 
					" " + udfList.get(0).getDescription());

			double minPerf = -1;
			long maxRuntime = 30 * 1000;

			TestHarness harness = new TestHarness(data, udfList, policy, learningTask, fname);
			try {
				String testName = label + "_baseline_" + numToProcess +"." + i; 
				harness.test(minPerf, maxRuntime, numToProcess, testName);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			learningTask.close();
		}
	}
	
	public static void main(String[] args) {
		BaselineNERTest baseline = new BaselineNERTest();
//		ExperimentParameters.set("mislabelArmCount", "1000");
//		ExperimentParameters.set("noisyFeatures", "true");
		ExperimentParameters.set("numClasses", "6");

//		ExperimentParameters.set("mislabelArmCount", "100");
//		ExperimentParameters.set("noisyFeatures", "true");
//		ExperimentParameters.set("noiseType", "ones");
		
		ExperimentParameters.set("rarity", "0.005"); 
		baseline.runTest(5, 10000, "ner_6class", true);
	}
}
