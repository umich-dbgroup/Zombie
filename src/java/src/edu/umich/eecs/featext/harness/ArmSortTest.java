package edu.umich.eecs.featext.harness;

public class ArmSortTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        int iterations = 10;
		int num = 10000;
		
        String prefix = args[0]; 
        String armSort = args[1];

		boolean reweight = true;

		ExperimentParameters.set("armSort", armSort);

		ContextualBanditTest contextBandit = new ContextualBanditTest();
		contextBandit.runTest(iterations, num, prefix, reweight);
       
	}

}
