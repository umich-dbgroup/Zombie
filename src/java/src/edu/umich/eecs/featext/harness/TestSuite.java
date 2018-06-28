package edu.umich.eecs.featext.harness;

public class TestSuite {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int iterations = 5;
		String test = args[0];
		int num = Integer.parseInt(args[1]);;
		String prefix = args[2]; //"lru_nosort_4class";

		boolean reweight = true;

		ExperimentParameters.set("rarity", "0.005");
		if (args.length > 3) {
			ExperimentParameters.set("mislabelArmCount", args[4]);
			ExperimentParameters.set("noisyFeatures", "true");
			ExperimentParameters.set("noiseType", args[3]);
			prefix += "_"+ args[3]+"_arms"+args[4];

		}

		ExperimentParameters.set("numItemsToProcess", "500000");

		ExperimentParameters.set("numClasses", "6");

		ExperimentParameters.set("indexType", "cluster");
		ExperimentParameters.set("clusterFile", "clusters_500.txt");

		if (test.equals("BaselineTest")) {
			BaselineTest baseline = new BaselineTest();
			baseline.runTest(iterations, num, prefix, reweight);
		}

		else if (test.equals("ProbDecisionBoundaryTest")) {
			ExperimentParameters.set("contextType","constant");
			ExperimentParameters.set("rewardType", "probDecisionBoundary");
			ExperimentParameters.set("alpha", "1.0");

			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, prefix+"_probBound", reweight);

			//            ExperimentParameters.set("randomizedIndex", "true");
			//		    contextBandit = new ContextualBanditTest();
			//		    contextBandit.runTest(iterations, num, prefix+"_rand_probBound", reweight);
			//
			//            ExperimentParameters.set("randomizedIndex", "false");
			//            ExperimentParameters.set("indexType", "w2v");
			//		    contextBandit = new ContextualBanditTest();
			//		    contextBandit.runTest(iterations, num, prefix+"_w2v_probBound", reweight);

		}
		else if (test.equals("ClassBalanceConstantTest")) {
			ExperimentParameters.set("contextType","constant");
			ExperimentParameters.set("rewardType", "labelCounts");
			ExperimentParameters.set("alpha", "1.0");
			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, prefix+"_classBal_const", reweight);

			//            ExperimentParameters.set("randomizedIndex", "true");
			//		    contextBandit = new ContextualBanditTest();
			//		    contextBandit.runTest(iterations, num, prefix+"_rand_classBal_const", reweight);
			//
			//            ExperimentParameters.set("randomizedIndex", "false");
			//            ExperimentParameters.set("indexType", "w2v");
			//		    contextBandit = new ContextualBanditTest();
			//		    contextBandit.runTest(iterations, num, prefix+"_w2v_classBal_const", reweight);
		}
		else if (test.equals("WrongClassUpdateTest")) {
			ExperimentParameters.set("contextType","constant");
			ExperimentParameters.set("rewardType", "wronglyClassified");
			ExperimentParameters.set("updateReward", "true");
			ExperimentParameters.set("alpha", "1.0");
			prefix = prefix + "_wrong_const_update";
			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, prefix, reweight);

			//            ExperimentParameters.set("randomizedIndex", "true");
			//		    contextBandit = new ContextualBanditTest();
			//		    contextBandit.runTest(iterations, num, prefix+"_rand_wrong_const_update", reweight);
			//
			//            ExperimentParameters.set("randomizedIndex", "false");
			//            ExperimentParameters.set("indexType", "w2v");
			//		    contextBandit = new ContextualBanditTest();
			//		    contextBandit.runTest(iterations, num, prefix+"_w2v_wrong_const_update", reweight);
		}

		else if (test.equals("ConstantTest")) {
			ExperimentParameters.set("contextType","constant");
			ExperimentParameters.set("rewardType", "constant");
			ExperimentParameters.set("alpha", "1.0");
			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, prefix+"_allconst", reweight);
		}
		else if (test.equals("RoundRobinTest")) {
			ExperimentParameters.set("contextType","constant");
			ExperimentParameters.set("rewardType", "roundRobin");
			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, prefix+"_roundrobin", reweight);
		}
		else if (test.equals("BaselineRegressionTest")) {
			ExperimentParameters.set("contextType", "constant");
			ExperimentParameters.set("rewardType", "constant");
			ExperimentParameters.set("numClasses", "6");
			ExperimentParameters.set("rarity", "0.005"); 
			BaselineRegressionTest baseline = new BaselineRegressionTest();
			baseline.runTest(iterations, num, prefix, reweight);
		}
		else if (test.equals("ZombieRegressionTest")) {
			ExperimentParameters.set("contextType", "constant");
			ExperimentParameters.set("rewardType", "wronglyClassified");
			ExperimentParameters.set("alpha", "1.0");
			ExperimentParameters.set("numClasses", "6");
			ExperimentParameters.set("rarity", "0.005"); 
			ContextualBanditRegressionTest baseline = new ContextualBanditRegressionTest();
			baseline.runTest(iterations, num, prefix, reweight);
		}
		else if (test.equals("Subset1")) {
			ExperimentParameters.set("contextType","constant");
			ExperimentParameters.set("rewardType", "constant");
			ExperimentParameters.set("alpha", "1.0");
			ExperimentParameters.set("armBudget", "360");
			ExperimentParameters.set("pruneType", "minorityItemCount");
			ExperimentParameters.set("doSubset", "true");
			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, prefix+"_subset01", reweight);
		}
		else if (test.equals("Subset5")) {
			ExperimentParameters.set("contextType","constant");
			ExperimentParameters.set("rewardType", "constant");
			ExperimentParameters.set("alpha", "1.0");
			ExperimentParameters.set("armBudget", "1800");
			ExperimentParameters.set("pruneType", "minorityItemCount");
			ExperimentParameters.set("doSubset", "true");
			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, prefix+"_subset05", reweight);
		}
		else if (test.equals("Subset10")) {
			ExperimentParameters.set("contextType","constant");
			ExperimentParameters.set("rewardType", "constant");
			ExperimentParameters.set("alpha", "1.0");
			ExperimentParameters.set("armBudget", "3600");
			ExperimentParameters.set("pruneType", "minorityItemCount");
			ExperimentParameters.set("doSubset", "true");
			ContextualBanditTest contextBandit = new ContextualBanditTest();
			contextBandit.runTest(iterations, num, prefix+"_subset10", reweight);
		}
		else if (test.equals("ClassifierTest")) {
			ClassifierTest cp = new ClassifierTest();
			cp.runTest(iterations, num, prefix, reweight);
		}
		else {
			System.out.println("Test not found.");
		}

		//ActiveLearningTest active = new ActiveLearningTest();
		//active.runTest(iterations, num, prefix, false);

		//ActiveUnlearningTest antilearn = new ActiveUnlearningTest();
		//antilearn.runTest(iterations, num, prefix, false);

		//        ReplayTest replay = new ReplayTest();
		//        replay.runTest(iterations, num, prefix, true);
		/*
        String label = "4class_entropy";
        String srcFile = "4class_entropy_1x_active_25000";

    /*
        int iterations = 5;
        String srcFile = args[0];
        int num = Integer.parseInt(args[1]);
        String label = srcFile; 
        boolean reweight = true;

        ReplayTest replay = new ReplayTest();
        replay.runTest(iterations, num, label, reweight, srcFile+"_"+num);
		 */
	}

}
