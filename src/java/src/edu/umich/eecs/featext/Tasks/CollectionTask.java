/**
 * 
 */
package edu.umich.eecs.featext.Tasks;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.jblas.DoubleMatrix;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.UDFs.UDF;
import edu.umich.eecs.featext.harness.ExperimentParameters;


/**
 * @author Mike Anderson
 *
 */
public class CollectionTask implements LearningTask {
	public static final String ID_STRING = "label_collection";

	ExperimentParameters params = new ExperimentParameters();

	String taskDesc;

	double performance = 0.0;

	private ProcessingResults procResults;

	HashMap<Integer, Double> itemPerformanceVals = new HashMap<Integer, Double>();
	List<Integer> itemIds = new ArrayList<Integer>();

	Labeler labeler;

	String[] labels;

	ArrayList<Double> featVals = new ArrayList<Double>();
	int featureCount = 0;

	boolean startingUp = true;
	ArrayList<String> udfNames = new ArrayList<String>();
	int udfCount = 0;
	int curUdf = udfCount - 1;

	ArrayList<UDF> udfList = new ArrayList<UDF>();

	HashMap<Integer, ArrayList<Double>> featureCache = new HashMap<Integer, ArrayList<Double>>();

	Random rand = new Random();

	/** Parameters / Config */

	private DataSource data;

	private PrintWriter featureWriter;

	private long labelTime;
	private long trainingTime;

	String labelToFind = "sports";
	List<Integer> foundItems = new ArrayList<Integer>();


	public CollectionTask(String taskDesc, DataSource data, ArrayList<UDF> udfs, String findLabel) {
		this.data = data;
		this.labeler = data.getLabeler();
		this.labelToFind = findLabel;

		labels = this.labeler.getLabels();

		this.taskDesc = taskDesc;

	}

	@Override
	public void provideData(Writable udfInputKey, Writable udfInputVal, GenericRecord udfOutput) {
		int itemId = ((IntWritable) udfInputKey).get();

		String[] featureNames = (String[]) udfOutput.get("featureNames");
		double[] udfVals = (double[]) udfOutput.get("value");
		long udfTime = (Long) udfOutput.get("execTime");

		performance = 0;

		int i = 0;
		for (String featName : featureNames) {
			if (featName.equals(labelToFind) && udfVals[i] > 0) {
				performance = 1;
				foundItems.add(itemId);
			}
			i++;
		}

		if (procResults == null || procResults.getItemId() != itemId) 
			procResults = new ProcessingResults(itemId);

		procResults.addUdfTime(udfTime);
		procResults.setFeatures(udfVals);
		procResults.setReward(performance);
	}

	@Override
	public double getPerformance() {
		return performance;// > 0.1 ? 1 : 0;
	}

	@Override
	public double[] getResults() {
		double[] output = new double[1];
		output[0] = foundItems.size();

		return output;
	}

	public double evaluateItem(IntWritable udfInputKey, Writable udfInputVal,
			GenericRecord udfOutput) {
		double value = 0;

		return value;
	}

	public Map<Integer, ClassPrediction> getEvaluationItems() {
		Map<Integer, ClassPrediction> items = new HashMap<Integer, ClassPrediction>();

		return items;
	}

	public Set<Integer> getTestSet() {
		return new HashSet<Integer>();
	}

	@Override
	public ProcessingResults getProcessingResults() {
		return procResults;
	}

	@Override
	public Map<Integer, Double> getAllItems() {
		return itemPerformanceVals;
	}

	@Override
	public String getDescription() {
		return ID_STRING;
	}

	@Override
	public DoubleMatrix getContext() {
		return getConstantContext();
	}

	public DoubleMatrix getConstantContext() {
		return DoubleMatrix.ones(1,1);
	}


	public double getUncertainty(int itemId, ArrayList<Double> featVals) {
		double value = 0;
		return value;
	}

	@Override
	public double getItemUncertainty(int itemId) {
		return 0.0;
	}


	@Override
	public HashMap<Integer, Double> getUncertainties(ArrayList<Integer> itemIds) {
		HashMap<Integer, Double> uncerts = new HashMap<Integer, Double>();
		return uncerts;
	}

	@Override
	public List<Integer> getItemIds() {
		return itemIds;
	}

	@Override
	public int[] getCounts() {
		int[] counts = new int[2];
		counts[0] = foundItems.size();
		counts[1] = itemIds.size() - foundItems.size();

		return counts;
	}

	@Override
	public void registerUDFs(ArrayList<UDF> udfList) {
		this.udfList = udfList;
	}

	@Override
	public String getContextType() {
		return "constant";
	}

	@Override
	public String getRewardType() {
		return "itemLabel";
	}

	@Override
	public long getLabelTime() {
		return labelTime;
	}

	@Override
	public long getTrainingTime() {
		return trainingTime;
	}

	@Override
	public void close() {
		//featureWriter.close();
	}

	@Override
	public void setFilePrefix(String prefix) {
		// TODO Auto-generated method stub

	}

	@Override
	public Double updateReward(int itemId) {
		return null;
	}

}
