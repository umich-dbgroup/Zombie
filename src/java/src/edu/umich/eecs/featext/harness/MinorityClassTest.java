package edu.umich.eecs.featext.harness;

public class MinorityClassTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int iterations = 10;
		int num = 50000;

		String prefix = args[0]; 
		String experiment = args[1];

		boolean reweight = true;

		String[] rarities = {"0.5", "0.1", "0.05", "0.01", "0.005", "0.001", "0.0005", "0.0001"};

		for (String rarity : rarities) {
			ExperimentParameters.set("rarity", rarity);
			
			if (experiment.equals("baseline")) {

				String fullPrefix = prefix + "_minClass-" + rarity;
				System.out.println(fullPrefix);

				SynthBaselineTest test = new SynthBaselineTest();
				test.runTest(iterations, num, fullPrefix, reweight);
			}
			else if (experiment.equals("context")) {
				String fullPrefix = prefix + "_minClass-" + rarity;
				System.out.println(fullPrefix);

				SynthContextualBanditTest test = new SynthContextualBanditTest();
				test.runTest(iterations, num, fullPrefix, reweight);
			}
			else if (experiment.equals("bySize")) {
				ExperimentParameters.set("armBudget", "30000");
				ExperimentParameters.set("pruneType", "bySize");

				String fullPrefix = prefix + "_minClass-" + rarity + "_size";
				System.out.println(fullPrefix);

				SynthContextualBanditTest test = new SynthContextualBanditTest();
				test.runTest(iterations, num, fullPrefix, reweight);
			}
			else if (experiment.equals("tfidf")) {
				ExperimentParameters.set("armBudget", "30000");
				ExperimentParameters.set("pruneType", "tfidf");

				String fullPrefix = prefix + "_minClass-" + rarity + "tfidf";
				System.out.println(fullPrefix);

				SynthContextualBanditTest test = new SynthContextualBanditTest();
				test.runTest(iterations, num, fullPrefix, reweight);
			}
		}

	}

}
