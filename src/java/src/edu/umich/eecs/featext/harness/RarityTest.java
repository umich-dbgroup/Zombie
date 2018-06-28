package edu.umich.eecs.featext.harness;

public class RarityTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        int iterations = 5;
		int num = 1000000;
		
        String prefix = args[0]; 
        //String armBudget = args[1];
        String rewardType = args[1];
        String numClasses = args[2];

		boolean reweight = true;

        //String[] budgets = {"1000", "10000", "15000", "20000", "25000", "30000", "35000", "40000", "45000", "50000", "55000", "60000"};
        //String[] budgets = {"10000", "20000", "30000", "40000", "50000", "60000", "70000", "80000", "90000", "100000"};
        String[] budgets = {"0.0001","0.0005","0.001","0.01","0.05"};
        //String[] budgets = {"0.005"};
        //String[] budgets = {"0.0001","0.0005", "0.001"};
        //String[] budgets = {"0.0001"};//,"0.001", "0.01"};

        for (String rarity : budgets) {

            ExperimentParameters.set("updateReward", "true");

		    ExperimentParameters.set("rarity", rarity);

            ExperimentParameters.set("numClasses", numClasses);
            ExperimentParameters.set("alpha", "2.0");

            if (rarity.equals("0.01") || rarity.equals("0.05")) {
                ExperimentParameters.set("numItemsToProcess", "25000");
            } else {
                ExperimentParameters.set("numItemsToProcess", "100000");
            }

            ExperimentParameters.set("numClasses", numClasses);

            ExperimentParameters.set("indexType", "cluster");
            ExperimentParameters.set("clusterFile", "clusters_500.txt");

            String fullPrefix = prefix + "_rarity-" + rarity + "_" + numClasses + "class";
            System.out.println(fullPrefix);

            if (rewardType.equals("baseline")) {
                BaselineTest baseline = new BaselineTest();
		        baseline.runTest(iterations, num, fullPrefix, reweight);
            } else {
                fullPrefix = fullPrefix + "_" + rewardType;
		        ExperimentParameters.set("rewardType", rewardType);
		        ExperimentParameters.set("contextType", "constant");
		        ContextualBanditTest contextBandit = new ContextualBanditTest();
		        contextBandit.runTest(iterations, num, fullPrefix, reweight);
            }
        }
       
	}

}
