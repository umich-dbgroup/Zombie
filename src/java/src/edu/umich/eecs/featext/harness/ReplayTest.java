package edu.umich.eecs.featext.harness;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.WikipediaDataSource;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Policies.ReplayPolicy;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.LogRegClassifierTask;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.UDF;


public class ReplayTest {
	void runTest(int iterations, int numToProcess, String label, boolean reweight, String srcFile) {
		for (int i = 1; i <= iterations; i++) {
            String replayFile = "test_data/output/" + srcFile + "." + i + "_ids.txt";

			ArrayList<UDF> udfList = new ArrayList<UDF>();

			udfList.add(KeyWordsUDF.createUDF("keywords"));

			DataSource data = new WikipediaDataSource();

			Policy policy = ReplayPolicy.createPolicy(replayFile);

            String taskDesc = "learningTask";
            if (reweight) taskDesc = "reweight";
            LearningTask learningTask = new LogRegClassifierTask(taskDesc, data, udfList);
			
            Path fname = new Path("test_data/wikipedia/wiki.seq");

            System.out.println("------------ ReplayTest ------------");
			System.out.println("Trial " + i +": " + learningTask.getDescription() + 
					" " + data.getDescription() + 
					" " + udfList.get(0).getDescription());

			double minPerf = -1;
			long maxRuntime = 30 * 1000;

			TestHarness harness = new TestHarness(data, udfList, policy, learningTask, fname);
			try {
				String testName = label + "_replay_" + numToProcess +"." + i; 
				harness.test(minPerf, maxRuntime, numToProcess, testName);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	public static void main(String[] args) {
		ReplayTest replay = new ReplayTest();
        int iterations = 5;
        int numToProcess = Integer.parseInt(args[1]);
        String label = args[0];
        boolean reweight = true;
        String srcFile = label + "_" + numToProcess;
        
		replay.runTest(iterations, numToProcess, label, reweight, srcFile);
	}
}
