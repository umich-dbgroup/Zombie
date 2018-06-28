package edu.umich.eecs.featext.Tasks;

public class ProcessingResults {
	public int itemId;
	public double udfTime; // in milliseconds
	public double reward;
	public double accuracy;
	public String label;
	public double[] features;

	public ProcessingResults(int itemId) {
		this.itemId = itemId;
		this.udfTime = 0.0;
		this.reward = 0.0;
	}
	
	public int getItemId() {
		return itemId;
	}
	
	public void setReward(double r) {
		reward = r;
	}
	
	public double getReward() {
		return reward;
	}
	
	public void addUdfTime(long nanoTime) {
		udfTime += (double) nanoTime/1000000;
	}
	
	public void addUdfTime(double u) {
		udfTime += u;
	}
	
	public double getUdfTime() {
		return udfTime;
	}
	
	public void setLabel(String l) {
		label = l;
	}
	
	public String getLabel() {
		return label;
	}
	
	public void setAccuracy(double acc) {
		accuracy = acc;
	}
	
	public double getAccuracy() {
		return accuracy;
	}
	
	public void setFeatures(double[] feats) {
		features = feats;
	}
	
	public double[] getFeatures() {
		return features;
	}
}
