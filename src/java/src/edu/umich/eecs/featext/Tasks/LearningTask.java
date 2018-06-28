package edu.umich.eecs.featext.Tasks;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.avro.generic.GenericRecord;
import org.jblas.DoubleMatrix;

import edu.umich.eecs.featext.UDFs.UDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/****************************************************
 * <code>LearningTask</code> encapsulates an in-progress machine
 * learning task, with data fed to it by the intelligent UDF system.
 *
 ****************************************************/
public interface LearningTask {
	public void provideData(Writable udfInputKey, Writable udfInputVal, GenericRecord udfOutput);
	public double getPerformance();

	
	public double[] getResults();
	
	public Set<Integer> getTestSet();
//	public Map<Integer, Double>  getRelevantFeatures();
	// Don't need this yet, but perhaps soon
	///public Iterator<GenericRecord> getDataSoFar();
	public double evaluateItem(IntWritable key, Writable contents,
			GenericRecord genericRecord);
	
	public Map<Integer, ClassPrediction> getEvaluationItems();
	
	public Map<Integer, Double> getAllItems();
	public String getDescription();
	public DoubleMatrix getContext();
	public int[] getCounts();
	public List<Integer> getItemIds();
	public double getItemUncertainty(int itemId);
	public HashMap<Integer, Double> getUncertainties(ArrayList<Integer> itemIds);
	public void registerUDFs(ArrayList<UDF> udfList);
	public ProcessingResults getProcessingResults();
	public String getContextType();
	public String getRewardType();
	public long getLabelTime();
	public long getTrainingTime();
	public void close();
	public void setFilePrefix(String prefix);
	public Double updateReward(int itemId);
}