package edu.umich.eecs.featext.harness;

public class VaryClassifierTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        int iterations = 5;
		int num = 1000000;
		
        String prefix = args[0]; 
        //String armBudget = args[1];
        String rewardType = "probDecisionBoundary";
        String numClasses = args[1];
        String numToProcess = args[2];
        String doNer = args[3];

		boolean reweight = true;

        String[] budgets = {"5000", "10000","20000","30000","40000","50000","60000","70000","80000","90000","100000"};

        for (String thresh : budgets) {

            ExperimentParameters.set("updateReward", "true");

		    ExperimentParameters.set("rarity", "0.005");
		    ExperimentParameters.set("trainingThreshold", thresh);

            ExperimentParameters.set("numClasses", numClasses);
            ExperimentParameters.set("alpha", "1.0");
    		ExperimentParameters.set("numItemsToProcess", numToProcess);
    		ExperimentParameters.set("doNER", doNer);
    		//
    		ExperimentParameters.set("indexType", "cluster");
    		ExperimentParameters.set("clusterFile", "clusters_500.txt");

            String fullPrefix = prefix + "_classifier-" + thresh + "_" + rewardType;
            System.out.println(fullPrefix);

            if (rewardType.equals("baseline")) {
                BaselineTest baseline = new BaselineTest();
		        baseline.runTest(iterations, num, fullPrefix, reweight);
            } else {
		        ExperimentParameters.set("rewardType", rewardType);
		        ExperimentParameters.set("contextType", "constant");
		        ClassifierTest contextBandit = new ClassifierTest();
		        contextBandit.runTest(iterations, num, fullPrefix, reweight);
            }
        }
       
	}

}
