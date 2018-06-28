package edu.umich.eecs.featext.harness;

public class HardnessTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        int iterations = 3;
		int num = 50000;
		
        String prefix = args[0]; 
        //String armBudget = args[1];

		boolean reweight = true;

        String[] budgets = {"0.005","0.002", "0.0005"};

		ExperimentParameters.set("contextType","decisionBoundary");
		ExperimentParameters.set("rewardType", "decisionBoundary");
		ExperimentParameters.set("timeSensitive", "true");
	    ExperimentParameters.set("armBudget", "40000");
	    ExperimentParameters.set("pruneType", "keepBigItems");

        for (String interval : budgets) {
        	ExperimentParameters.set("rarity", interval);
        	
            String fullPrefix = prefix + "_hard-" + interval;
            System.out.println(fullPrefix);

		    ContextualBanditTest contextBandit = new ContextualBanditTest();
		    contextBandit.runTest(iterations, num, fullPrefix, reweight);

        }
       
	}

}
