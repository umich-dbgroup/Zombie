package edu.umich.eecs.featext.harness;

public class TrainingIntervalTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        int iterations = 3;
		int num = 50000;
		
        String prefix = args[0]; 
        //String armBudget = args[1];

		boolean reweight = true;

        String[] budgets = {"50", "100", "500", "1000"};
		ExperimentParameters.set("contextType","decisionBoundary");
		ExperimentParameters.set("rewardType", "decisionBoundary");
		ExperimentParameters.set("timeSensitive", "true");
	    ExperimentParameters.set("armBudget", "40000");
	    ExperimentParameters.set("pruneType", "keepBigItems");
	    ExperimentParameters.set("rarity", "0.001");
        for (String interval : budgets) {
        	ExperimentParameters.set("trainingInterval", interval);
        	
            String fullPrefix = prefix + "_interval-" + interval;
            System.out.println(fullPrefix);

		    ContextualBanditTest contextBandit = new ContextualBanditTest();
		    contextBandit.runTest(iterations, num, fullPrefix, reweight);
        }
       
	}

}
