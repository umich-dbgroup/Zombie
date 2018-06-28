package edu.umich.eecs.featext.harness;

public class ArmBudgetTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int iterations = 5;
		int num = 1000000;

		String prefix = args[0]; 
		//String armBudget = args[1];
		String pruneType = args[1];

		boolean reweight = true;

		//String[] budgets = {"1000", "10000", "15000", "20000", "25000", "30000", "35000", "40000", "45000", "50000", "55000", "60000"};
		//String[] budgets = {"10000", "20000", "30000", "40000", "50000", "60000", "70000", "80000", "90000", "100000"};
		String[] budgets = {"5000","10000","15000","20000","25000","30000"};

		for (String armBudget : budgets) {
			ExperimentParameters.set("armBudget", armBudget);
			ExperimentParameters.set("pruneType", pruneType);
			ExperimentParameters.set("rewardType", "wronglyClassified");
			ExperimentParameters.set("contextType", "constant");

			ExperimentParameters.set("rarity", "0.005");


			ExperimentParameters.set("numClasses","6");
			ExperimentParameters.set("alpha", "1.0");

			String fullPrefix = prefix + "_arms-" + armBudget + "_" + pruneType;
			System.out.println(fullPrefix);

			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, fullPrefix, reweight);
		}

	}

}
