package edu.umich.eecs.featext.harness;

public class SynthArmBudgetTest {

	/**
	 * @param args
	 */
	
	// supported prune types: bySize, random, tfidf
	public static void main(String[] args) {
        int iterations = 10;
		int num = 10000;
		
        String prefix = args[0]; 
        String pruneType = args[1];

		boolean reweight = true;

        String[] budgets = {"100", "10000", "20000", "30000", "40000", "50000", "60000", "70000", "80000", "90000"};

        for (String armBudget : budgets) {
		    ExperimentParameters.set("armBudget", armBudget);
		    ExperimentParameters.set("pruneType", pruneType);

            String fullPrefix = prefix + "_arms-" + armBudget + "_" + pruneType;
            System.out.println(fullPrefix);

		    SynthContextualBanditTest test = new SynthContextualBanditTest();
		    test.runTest(iterations, num, fullPrefix, reweight);
        }
       
	}

}
