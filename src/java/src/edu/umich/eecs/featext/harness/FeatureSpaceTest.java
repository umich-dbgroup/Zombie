package edu.umich.eecs.featext.harness;

public class FeatureSpaceTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        int iterations = 10;
		int num = 100000;
		
        String prefix = args[0]; 

		boolean reweight = true;

        BaselineTest baseline = new BaselineTest();
        baseline.runTest(iterations, num, prefix, reweight);

        ContextualBanditTest contextBandit = new ContextualBanditTest();
        contextBandit.runTest(iterations, num, prefix, reweight);

        ExperimentParameters.set("contextType","featureSpace");
        ExperimentParameters.set("rewardType", "featureSpace");

        contextBandit = new ContextualBanditTest();
        contextBandit.runTest(iterations, num, "_fs_" + prefix, reweight);
       
	}

}
