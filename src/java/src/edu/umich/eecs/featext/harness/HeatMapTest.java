package edu.umich.eecs.featext.harness;

public class HeatMapTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int iterations = 10;
		int num = 100000;

		boolean reweight = true;

		String[] values = {"0.1"," 0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"};

		for (String purity : values) {
			for (String density : values) {

				ExperimentParameters.set("alpha", "1.0");
				ExperimentParameters.set("rarity", "0.005");

				ExperimentParameters.set("numClasses", "6");
				ExperimentParameters.set("numItemsToProcess", "100000");

				ExperimentParameters.set("indexType", "heatMapClusters");
				ExperimentParameters.set("heatMapPurity", purity);
				ExperimentParameters.set("heatMapDensity", density);

				String reward = "probDecisionBoundary";

				String fullPrefix = "heatmap_p-" + purity + "_d-" + density + "_" + reward;
				System.out.println(fullPrefix);

				ExperimentParameters.set("rewardType", reward);
				ExperimentParameters.set("contextType", "constant");
				ContextualBanditTest contextBandit = new ContextualBanditTest();
				contextBandit.runTest(iterations, num, fullPrefix, reweight);
			}
		}

	}

}
