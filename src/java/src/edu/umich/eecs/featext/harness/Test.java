package edu.umich.eecs.featext.harness;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.WikipediaDataSource;
import edu.umich.eecs.featext.Policies.ClassifierPolicy;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.LogRegClassifierTask;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.UDF;


public class Test {
	public static void main(String[] args) {
		for (int i = 1; i <= 5; i++) {
			ArrayList<UDF> udfList = new ArrayList<UDF>();
			
//			udfList.add(CapitalUDF.createUDF("classify"));
//			udfList.add(InEuropeUDF.createUDF("classify"));
//			udfList.add(OlympicsUDF.createUDF("classify"));
//			udfList.add(DocLengthUDF.createUDF("classify"));
//			udfList.add(HasBallUDF.createUDF("classify"));
			
				udfList.add(KeyWordsUDF.createUDF("keywords"));
			
//			udfList.add(IsBrightImageUDF.createUDF("foo"));
//			udfList.add(IsDarkImageUDF.createUDF("foo"));
//			udfList.add(IsRedImageUDF.createUDF("foo")); 
//			udfList.add(IsGreenImageUDF.createUDF("foo"));
//			udfList.add(IsBlueImageUDF.createUDF("foo"));

			DataSource data = new WikipediaDataSource();
			Policy policy = ClassifierPolicy.createPolicy("uncertainty");
			//    LearningTask learningTask = new ClassBalancerTask("balancer");
			LearningTask learningTask = new LogRegClassifierTask("naivebayes");
			Path fname = new Path("test_data/wikipedia/wiki.seq");
			
			System.out.println("Trial " + i +": " + learningTask.getDescription() + 
					               " " + data.getDescription() + 
					               " " + udfList.get(0).getDescription());
			
			double minPerf = -1;
			long maxRuntime = 30 * 1000;
			int numToProcess = 10000;

			TestHarness harness = new TestHarness(data, udfList, policy, learningTask, fname);
			try {
				String testName = "test." + i; //"bandit_wiki_olympics_europe_balancer" + i;
				harness.test(minPerf, maxRuntime, numToProcess, testName);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
