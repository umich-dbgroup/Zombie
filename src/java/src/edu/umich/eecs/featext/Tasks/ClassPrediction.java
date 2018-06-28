package edu.umich.eecs.featext.Tasks;

import java.util.Arrays;

public class ClassPrediction {
	public final int index;
	public final double[] classProbs;
	public final double prob;
	public final String label;
	public final String trueLabel;

	public ClassPrediction(double[] classProbs, String[] labels) {
		// find the most probable for this class
		double maxProb = -1.0;
		int idx = -1;
		for (int i = 0; i < classProbs.length; i++) {
			if (classProbs[i] > maxProb) {
				maxProb = classProbs[i];
				idx = i;
			}
		}
		
		this.classProbs = classProbs;
		prob = maxProb;
		index = idx;
		label = labels[idx];
		trueLabel = "";
	}
	
	public ClassPrediction(double[] classProbs, String[] labels, String groundTruth) {
		// find the most probable for this class
		double maxProb = -1.0;
		int idx = -1;
		for (int i = 0; i < classProbs.length; i++) {
			if (classProbs[i] > maxProb) {
				maxProb = classProbs[i];
				idx = i;
			}
		}
		
		this.classProbs = classProbs;
		prob = maxProb;
		index = idx;
		label = labels[idx];
		trueLabel = groundTruth;
	}
	
	public double getUncertainty() {
		return 1.0 - prob;
	}
	
	public double getMargin() {
		double[] vals = Arrays.copyOf(classProbs, classProbs.length);
		Arrays.sort(vals);
		return vals[vals.length-1] - vals[vals.length-2];
	}
	
	public double getEntropy() {
		double entropy = 0.0;
		for (double p : classProbs) {
			if (p > 0) {
				entropy += p * Math.log(p);
			}
		}
		return -entropy;
	}
	
	public boolean isCorrect() {
		return label.equals(trueLabel);
	}
}
