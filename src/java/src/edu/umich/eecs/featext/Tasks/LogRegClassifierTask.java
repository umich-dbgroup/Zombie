/**
 * 
 */
package edu.umich.eecs.featext.Tasks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.jblas.DoubleMatrix;
import org.jblas.Geometry;

import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayesMultinomial;
import weka.classifiers.bayes.NaiveBayesMultinomialUpdateable;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.functions.Logistic;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;
import weka.core.converters.ArffSaver;

import com.google.common.base.Joiner;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.UDF;
import edu.umich.eecs.featext.harness.ExperimentParameters;


/**
 * @author Mike Anderson
 *
 */
public class LogRegClassifierTask implements LearningTask {
	public static final String ID_STRING = "logistic_regression";

	ExperimentParameters params = new ExperimentParameters();

	Set<Integer> testSet = new HashSet<Integer>();
	List<Integer> testIds = new ArrayList<Integer>();
	ArrayList<Integer> trainIds = new ArrayList<Integer>();
	ArrayList<Integer> evalIds = new ArrayList<Integer>();
	ArrayList<Integer> stoppingIds = new ArrayList<Integer>();

	int amtPerClass = 2500; //1250;
	String taskDesc;

	private HashMap<Integer, Integer> featureMap = new HashMap<Integer, Integer>();
	private ArrayList<Integer> featureList = new ArrayList<Integer>();
	private HashMap<Integer, Integer> featureLocs = new HashMap<Integer, Integer>();

	private Instances wekaTest; // weka version of test set
	private Instances wekaTrain;
	private Instances wekaEval; // for experiments
	private Instances wekaStopping; // to measure stopping criterion via uncertainty

	NaiveBayesMultinomialUpdateable cModel;

	double performance = 0.0;
	private double pctCorrect;
	int stepsUntilRetrain = 1;
	int trainingStepSize = Integer.valueOf(ExperimentParameters.get("trainingInterval", "20"));

	private ProcessingResults procResults;

	int mostUncertainIdx = 0;
	double mostUncertainVal = 10.0;

	HashMap<Integer, Double> itemPerformanceVals = new HashMap<Integer, Double>();
	List<Integer> itemIds = new ArrayList<Integer>();

	//ImageLabeler labeler = new ImageLabeler("test_data/Caltech101/imageFiles.txt");
	Labeler labeler;

	String[] labels;

	HashMap<String, Integer> labelCounts = new HashMap<String, Integer>();
	HashMap<String, Integer> stepsSinceSeen = new HashMap<String, Integer>();

	ArrayList<Double> featVals = new ArrayList<Double>();
	int featureCount = 0;

	boolean startingUp = true;
	ArrayList<String> udfNames = new ArrayList<String>();
	int udfCount = 0;
	int curUdf = udfCount - 1;

	ArrayList<UDF> udfList = new ArrayList<UDF>();

	HashMap<Integer, ArrayList<Double>> featureCache = new HashMap<Integer, ArrayList<Double>>();

	Random rand = new Random();

	private Evaluation eTest;

	/** Parameters / Config */

	boolean doReweighting = false;
	double stoppingPercentage = 0.0; // amount of holdout set to use to compute stopping point

	private String itemLabel;
	private String previousLabel;

	private double aucRoc;

	String rewardType;
	String contextType;

	private DoubleMatrix lastContext;
	private String itemPredictedLabel = "none";

	private DataSource data;

	private PrintWriter featureWriter;

	private Joiner joiner;

	private long labelTime;
	private long trainingTime;

	private int numRetrains = 0;

	private boolean doInitialSearch = true;

	private ClassPrediction itemPrediction;

	public LogRegClassifierTask(String taskDesc) {
		this.taskDesc = taskDesc;

		if (taskDesc.equals("reweight")) doReweighting = true;
		int i = 0;
		for (String lbl : labels) {
			labelCounts.put(lbl, 0);
			stepsSinceSeen.put(lbl, i++);
		}

		rewardType = ExperimentParameters.get("rewardType");
		if (rewardType == null) {
			rewardType = "booleanLabelCounts";
		}

		contextType = ExperimentParameters.get("contextType");
		if (contextType == null) {
			contextType = "classBalance";
		}

		if (ExperimentParameters.get("trainingInterval") != null) {
			trainingStepSize = Integer.valueOf(ExperimentParameters.get("trainingInterval"));
		}

		buildTestSet();

	}

	public LogRegClassifierTask(String taskDesc, DataSource data, ArrayList<UDF> udfs) {
		this.data = data;

		this.labeler = data.getLabeler();
		labels = this.labeler.getLabels();

		this.taskDesc = taskDesc;

		if (taskDesc.equals("reweight")) doReweighting = true;
		int i = 0;
		for (String lbl : labels) {
			labelCounts.put(lbl, 0);
			stepsSinceSeen.put(lbl, i++);
		}

		rewardType = ExperimentParameters.get("rewardType");
		if (rewardType == null) {
			rewardType = "labelCounts";
		}

		contextType = ExperimentParameters.get("contextType");
		if (contextType == null) {
			contextType = "classBalance";
		}

		registerUDFs(udfs);
		buildTestSet();

		/////*******************************************************************
		try {
			featureWriter = new PrintWriter("features.txt", "UTF-8");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		joiner = Joiner.on(",").skipNulls();
		/////*******************************************************************

	}

	@Override
	public void provideData(Writable udfInputKey, Writable udfInputVal,
			GenericRecord udfOutput) {
		try {
			//double featVal = new Double(udfOutput.get("value").toString());
			int itemId = ((IntWritable) udfInputKey).get();

			double[] udfVals = (double[]) udfOutput.get("value");
			long udfTime = (Long) udfOutput.get("execTime");

			if (procResults == null || procResults.getItemId() != itemId) 
				procResults = new ProcessingResults(itemId);

			procResults.addUdfTime(udfTime);
			procResults.setFeatures(udfVals);

			String udfName = udfOutput.getSchema().getName();
			itemPerformanceVals = new HashMap<Integer, Double>();

			if (startingUp) {
				// Assumption: if we see a udfName again, we've seen them all, 
				// so we can stop collecting them.
				if (udfNames.contains(udfName)) {
					startingUp = false;
					udfCount = udfNames.size();
				}
				else {
					udfNames.add(udfName);
				}
			}


			for (double v : udfVals) {
				featVals.add(v);
			}

			// Once we get all the features we are expecting, add this item
			// to the training set and evaluate it for feedback to the policy.
			if (featVals.size() == featureCount) {
				long startLabelTime = System.nanoTime();
				itemLabel = labeler.getLabel(itemId);
				labelTime = System.nanoTime() - startLabelTime;

				boolean isGoodLabel = false;
				int labelId = 0;
				int i = 0;
				for (String lbl : labels) {
					if (itemLabel.equals(lbl)) {
						isGoodLabel = true;
						labelId = i;
					}
					i++;
				}

				//featureWriter.println(Joiner.on(",").join(featVals) + "," + labelId);

				performance = 0;
				if (isGoodLabel) {			
					labelCounts.put(itemLabel, labelCounts.get(itemLabel) + 1);

					// We only start training when we have a "non-other" example
					if (doInitialSearch) {
						int minorityExamples = 0;
						for (String lbl : labels) {
							if (!lbl.equals("other"))
								minorityExamples += labelCounts.get(lbl);
						}
						if (minorityExamples > 0) {
							doInitialSearch = false;
							stepsUntilRetrain = 1; // retrain now that we have a good example
						}
					}				

					if (wekaTrain.size() == 0 || cModel == null) {
						cModel = new NaiveBayesMultinomialUpdateable(); //new NaiveBayes();
						//cModel.buildClassifier(wekaTrain);
						stepsUntilRetrain--;

						addToTrainingSet(itemId, featVals, true); //(stepsUntilRetrain == 0));
						performance = 1;
					}
					else {
						performance = 1;
						stepsUntilRetrain--;

						Instance itemInstance = createInstance(itemId, featVals);
						double prediction = eTest.evaluateModelOnce(cModel, itemInstance);

						itemPrediction = new ClassPrediction(cModel.distributionForInstance(itemInstance), labels, itemLabel);

						itemPredictedLabel = itemPrediction.label;

						trainingTime = 0; // this is populated in the following method call
						addToTrainingSet(itemId, featVals, (stepsUntilRetrain == 0));
					}
					//System.out.println(joiner.join(featVals));

					// Train classifier
					if (wekaTrain.size() > 0) {

						if (stepsUntilRetrain == 0) {
							numRetrains++;

							if (rewardType.equals("aucRoc") || rewardType.equals("contextDistance")) {
								stepsUntilRetrain = 1;
							}
							else if (itemIds.size() < 5000) {
								stepsUntilRetrain = trainingStepSize;
							} else if (itemIds.size() < 10000) {
								stepsUntilRetrain = trainingStepSize*2;
							} else {
								stepsUntilRetrain = trainingStepSize*20;
							}



							// Test classifier
							eTest = new Evaluation(wekaTest);
							eTest.evaluateModel(cModel, wekaTest);		

							pctCorrect = eTest.pctCorrect();

							featureCache.put(itemId, featVals);

							//eTest.crossValidateModel(cModel, wekaTrain, 10, rand);
							/*
						System.out.println("------------------");
						for (int i = 0; i < labels.length; i++) {
							System.out.println(String.format("%-10s:  %.3f, %.3f", labels[i], eTest.precision(i), eTest.recall(i)));
						}

						System.out.println("------------------");
							 */
						}



						if (rewardType.equals("aucRoc")) {
							performance = performanceAucRoc();
						}
						else if (rewardType.equals("contextDistance")) {
							performance = performanceContextDistance();
						}
						else if (rewardType.equals("labelCounts")) {
							performance = performanceLabelCounts();
						}						
						else if (rewardType.equals("booleanLabelCounts")) {
							performance = performanceLabelCountsBoolean();
						}
						else if (rewardType.equals("wronglyClassified")) {
							performance = performanceWronglyClassified();
						}	
						else if (rewardType.equals("weightedWronglyClassified")) {
							performance = performanceWeightedWronglyClassified();
						}	
						else if (rewardType.equals("decisionBoundary")) {
							performance = performanceDecisionBoundary(featVals);
						}								
						else if (rewardType.equals("probDecisionBoundary")) {
							performance = performanceProbDecisionBoundary();
						}	
						else if (rewardType.equals("weightedProbDecisionBoundary")) {
							performance = performanceWeightedProbDecisionBoundary();
						}	
						else if (rewardType.equals("probWronglyClassified")) {
							performance = performanceProbWronglyClassified();
						}						
						else if (rewardType.equals("weightedDecisionBoundary")) {
							performance = performanceWeightedDecisionBoundary(featVals);
						}
						else if (rewardType.equals("constant")) {
							performance = performanceConstant();
						}
						else if (rewardType.equals("roundRobin")) {
							performance = performanceRoundRobin();
						}
						else {
							throw new RuntimeException("Unknown reward type: " + rewardType);	
						}

						itemPerformanceVals.put(itemId, performance);

						procResults.setReward(performance);
						procResults.setAccuracy(pctCorrect);
						procResults.setLabel(itemLabel);

						itemIds.add(itemId);
					}
				}
				else {
					itemPerformanceVals.put(itemId, performance);
					procResults.setReward(performance);

				}
				featVals = new ArrayList<Double>();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public double getPerformance() {
		return performance;// > 0.1 ? 1 : 0;
	}

	@Override
	public double[] getResults() {
		double[] output = new double[labelCounts.size() + 2];
		output[0] = pctCorrect;
		output[1] = getStoppingValue();
		int i = 2;
		for (String lbl : labels) {
			output[i] = labelCounts.get(lbl);
			i++;
		}
		return output;
	}

	public double evaluateItem(IntWritable udfInputKey, Writable udfInputVal,
			GenericRecord udfOutput) {
		double value = 0;

		return value;
	}

	public Map<Integer, ClassPrediction> getEvaluationItems() {
		Map<Integer, ClassPrediction> items = new HashMap<Integer, ClassPrediction>();

		// we will return the classification probabilities for 
		// the combination of the test and training sets, so they
		// can all be used by the policy to determine the next to send
		Evaluation eTest;
		try {
			eTest = new Evaluation(wekaTrain);
			eTest.evaluateModel(cModel, wekaTest);

			for (int i = 0; i < testIds.size(); i++) {
				Instance inst = wekaTest.instance(i);
				Attribute attr = inst.classAttribute();
				items.put(testIds.get(i), new ClassPrediction(cModel.distributionForInstance(inst), labels, attr.value(0)));
			}
			//			String[] lbls = {"0", "1"};
			//			
			//			for (int i = 0; i < testIds.size(); i++) {
			//				double[] vals = new double[2];
			//				
			//			}	
			//			eTest = new Evaluation(wekaTrain);
			//			eTest.evaluateModel(cModel, wekaTrain);
			//	
			//			for (int i = 0; i < trainIds.size(); i++) {
			//				Instance inst = wekaTrain.instance(i);
			//				Attribute attr = inst.classAttribute();
			//				items.put(trainIds.get(i), new ClassPrediction(cModel.distributionForInstance(inst), labels, attr.value(0)));
			//			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return items;
	}


	/**
	 * Build a test set from the full list of document IDs by selecting a random
	 * $testSetSize of them.
	 */
	private void buildTestSet() {
		System.out.println("Building test set.");


		// Start building the Instances		
		Attribute cls = new Attribute("class", new ArrayList<String>(Arrays.asList(labels)));

		// Build a list of all features we will use, so we can use the list
		// index as a temporary feature ID.
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();

		// Get a sample output from each UDF so we can build up the attributes
		// We will assume the ordering of udfList and the output of the UDFs
		// is stable such that they will always be in the same order.
		for (UDF udf : udfList) {
			IntWritable key = new IntWritable();
			Writable contents = new Text();
			GenericRecord output = udf.createOutput(key, contents);
			String udfName = output.getSchema().getName();
			String[] featureNames = (String[]) output.get("featureNames");
			for (String featureName : featureNames) {
				attributes.add(new Attribute(udfName+":"+featureName));
				featureCount++;
			}
		}

		attributes.add(cls);
		wekaTest = new Instances("Dataset", attributes, 0);
		wekaTest.setClass(cls);

		// Initialize the training set here, too.
		wekaTrain = new Instances("Dataset", attributes, 0);
		wekaTrain.setClass(cls);

		wekaEval = new Instances("Dataset", attributes, 0);
		wekaEval.setClass(cls);

		wekaStopping = new Instances("Dataset", attributes, 0);
		wekaStopping.setClass(cls);

		// Now go through IDs in the training set and actually add them as
		// example instances with the temporary feature IDs figured out above.
		System.out.println("Building weka test set.");
		List<Item> testItems = data.getTestItems(2000, null);
		List<Item> stoppingItems = data.getStoppingItems(1000, null);

		boolean wekaTestLoaded = false;

		String arffFileName = "wekaTest.arff";
		// jobId is from demo
		if (ExperimentParameters.get("jobId") != null) {
			arffFileName = "wekaTest.arff.job-" + ExperimentParameters.get("jobId");
		}
		else if (ExperimentParameters.check("numClasses", "2"))
			arffFileName = "wekaTest.arff.2class";
		else if (ExperimentParameters.check("numClasses", "6"))
			arffFileName = "wekaTest.arff.6class";


		File f = new File(arffFileName);
		if(f.exists() && !f.isDirectory()) {
			try {
				System.out.println("Using " + arffFileName);
				BufferedReader reader;
				reader = new BufferedReader(new FileReader(f));
				wekaTest = new Instances(reader);
				wekaTest.setClass(cls);

				reader.close();
				wekaTestLoaded = true;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		for (Item item : testItems) {
			int id = item.getItemId();
			testIds.add(id);
			testSet.add(id);

			if (!wekaTestLoaded) {
				double[] values = new double[wekaTest.numAttributes()];

				int i = 0;
				try {
					for (UDF udf : udfList) {
						GenericRecord udfOutput = udf.createOutput(new IntWritable(item.getItemId()), new Text(data.getDocumentContents(id)));
						for (double val : (double[]) udfOutput.get("value")) {
							values[i] = val;
							i++;
						}
					}
					Instance inst = new SparseInstance(1.0, values);
					inst.setDataset(wekaTest);		

					// Get the text of the doc so we can classify it. 
					// TODO: this should be more general
					inst.setClassValue(labeler.getLabel(id));

					wekaTest.add(inst);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		if (!wekaTestLoaded) {
			try {
				ArffSaver saver = new ArffSaver();
				saver.setInstances(wekaTest);
				saver.setFile(new File(arffFileName));
				saver.setDestination(new File(arffFileName));
				saver.writeBatch();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}   // **not** necessary in 3.5.4 and later
		}

		for (Item item : stoppingItems) {
			int id = item.getItemId();
			stoppingIds.add(id);
			double[] values = new double[wekaStopping.numAttributes()];

			try {
				values = KeyWordsUDF.getValue(id);

				Instance inst = new SparseInstance(1.0, values);
				inst.setDataset(wekaStopping);	

				wekaStopping.add(inst);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("Done building test set.");
	}

	private Instance createInstance(int docId, ArrayList<Double> featVals) {
		String cls = labeler.getLabel(docId);

		Instance inst = null;
		try {
			double[] values = new double[wekaTrain.numAttributes()];

			for (int i = 0; i < featVals.size(); i++) {		
				values[i] = featVals.get(i);
			}

			inst = new SparseInstance(1.0, values);
			inst.setDataset(wekaTrain);
			inst.setClassValue(cls);
			//System.out.println(docId + " -- " + inst);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//System.out.println("**** bad label: " + cls);
		}					
		return inst;
	}

	private void addToTrainingSet(int docId, ArrayList<Double> featVals, boolean retrain) {
		Instance inst = createInstance(docId, featVals);
		if (inst != null) {

			wekaTrain.add(inst);
			trainIds.add(docId);
			//if (retrain) {
				long startTrain = System.nanoTime();
				try {
					if (wekaTrain.size() > 1) {
					cModel.updateClassifier(inst);
					//if (!doInitialSearch && doReweighting) reweightTrainingSet();
					} else {
					cModel = new NaiveBayesMultinomialUpdateable();
					wekaTrain.randomize(rand);
					cModel.buildClassifier(wekaTrain);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				trainingTime = System.nanoTime() - startTrain;
			//}
		}
	}

	private void reweightTrainingSet() {
		double[] weights = new double[labels.length];
		double min = 999999999;

		int[] tempCounts = {60, 9940};

		// First, find the min and set weights to the reciprocal count
		for (int i = 0; i < labels.length; i++) {
			if (labelCounts.get(labels[i]) < min) {
				//if (tempCounts[i] < min) {
				min = labelCounts.get(labels[i]);
				//min = tempCounts[i];
			}
			if (labelCounts.get(labels[i]) > 0) 
				weights[i] = 1.0/((double) labelCounts.get(labels[i]));
			//weights[i] = 1.0/((double) tempCounts[i]);
			else
				weights[i] = 1.0;
		}
		if (min == 0) min = 1;

		// Now multiply by the min to get the appropriately weighted value
		for (int i = 0; i < weights.length; i++) {
			weights[i] *= min;
		}



		// Go back and reweight the instances. We assume the class value equals
		// the original index in the labels array (and therefore the weights array).
		for (int i = 0; i < wekaTrain.size(); i++) {
			wekaTrain.instance(i).setWeight(weights[(int) wekaTrain.instance(i).classValue()]);
		}
	}

	public Set<Integer> getTestSet() {
		return testSet;
	}

	@Override
	public ProcessingResults getProcessingResults() {
		Object procResult;
		ProcessingResults toReturn = procResults;
		procResults = null;
		return toReturn;
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
		String contextType = ExperimentParameters.get("contextType");
		if (contextType == null) contextType = "NULL";

		if (contextType.equals("featureVariance")) {
			return getFeatureVarianceContext();
		}
		else if (contextType.equals("constant")) {
			return getConstantContext();
		}
		else if (contextType.equals("decisionBoundary")) {
			return getDecisionBoundaryContext();
		}
		else if (contextType.equals("itemCount")) {
			return getItemCountContext();
		}
		else if (contextType.equals("classBalance")) {
			return getClassBalanceContext();
		}		
		else if (contextType.equals("classBalanceBoolean")) {
			return getClassBalanceBooleanContext();
		}
		else {
			throw new RuntimeException("Unknown context: " + contextType);
		}
	}

	public DoubleMatrix getConstantContext() {
		return DoubleMatrix.ones(1,1);
	}

	public DoubleMatrix getItemCountContext() {
		DoubleMatrix context = new DoubleMatrix(2,1);
		context.put(0, 1.0); //intercept
		context.put(1, wekaTrain.size());
		return context;
	}

	public DoubleMatrix getClassBalanceContext() {
		DoubleMatrix context = DoubleMatrix.zeros(labels.length, 1);
		double[] counts = new double[labels.length];
		int sum = 0;


		for (int i = 0; i < labels.length; i++) {
			if (labels[i].equals(previousLabel)) 
				counts[i] = 0.0;
			else 
				counts[i] = labelCounts.get(labels[i]);
			sum += counts[i];
		}

		if (sum == 0) context = DoubleMatrix.ones(labels.length, 1);
		else {
			for (int i = 0; i < labels.length; i++) {
				context.put(i, counts[i]/sum);
			}
		}

		return context;
	}

	public DoubleMatrix getClassBalanceBooleanContext() {
		DoubleMatrix context = DoubleMatrix.ones(labels.length, 1);
		List<Integer> counts = new ArrayList<Integer>();

		for (int i = 0; i < labels.length; i++) {
			if (labels[i].equals(previousLabel)) 
				counts.add(0);
			else 
				counts.add(labelCounts.get(labels[i]));
		}

		int min = Collections.min(counts);
		int max = Collections.max(counts);
		if (min < max) {
			for (int i = 0; i < labels.length; i++) {
				if (counts.get(i) == min) context.put(i, 0);
			}
		}
		return context;
	}

	public DoubleMatrix getClassBalanceLRUContext() {
		DoubleMatrix context = DoubleMatrix.zeros(labels.length, 1);
		/*
		int sum = 0;

		for (int i = 0; i < labels.length; i++) {
			sum += stepsSinceSeen.get(labels[i]);
		}

		if (sum == 0) context = DoubleMatrix.ones(2 * labels.length, 1);
		else {
			for (int i = 0; i < labels.length; i++) {
				context.put(i, (double) stepsSinceSeen.get(labels[i])/sum);
			}
		}
		 */

		for (int i = 0; i < labels.length; i++) {
			int numHigher = 0;
			for (int j = 0; j < labels.length; j++) {
				if (stepsSinceSeen.get(labels[j]) > stepsSinceSeen.get(labels[i])) {
					numHigher++;
				}
			}
			//context.put(i, 1.0 - (double) numHigher/(labels.length - 1));
			if (numHigher == 0)
				context.put(i, 1.0);
			else
				context.put(i, 0.0);
		}

		return context;
		//			return Geometry.normalize(context);
	}

	public DoubleMatrix getFeatureVarianceContext() {
		DoubleMatrix context = new DoubleMatrix(wekaTrain.numAttributes(), 1);

		for (int i = 0; i < wekaTrain.numAttributes(); i++) {
			int n = 0;
			double mean = 0.0;
			double M2 = 0.0;

			for (int j = 0; j < wekaTrain.numInstances(); j++) {
				double x = wekaTrain.instance(j).value(i);
				n = n + 1;
				double delta = x - mean;
				mean = mean + delta/n;
				M2 = M2 + delta*(x - mean);
			}
			if (n > 1) context.put(i, M2/(n - 1));
			else context.put(i, 1.0);
		}
		context = Geometry.normalize(context);
		return context;
	}

	public DoubleMatrix getDecisionBoundaryContext() {
		DoubleMatrix context = new DoubleMatrix(wekaTrain.numAttributes(), 1);

		// We may need to do something differently initially, since the coefficients
		// returned are all 0 except the intercept, which makes no sense in 
		// defining a hyperplane.

		for (int i = 0; i < context.length; i++) {
			context.put(i, 0.1);
		}

//		if (cModel != null) {
//			int i = 0;
//			int coefToUse = numRetrains % (labels.length-1);
//			for (double[] coef : cModel.coefficients()) {
//				if (Math.abs(coef[coefToUse]) < 0.001) context.put(i, 0.1);
//				else context.put(i, coef[coefToUse]);
//				//				if (Math.abs(coef[coefToUse]) > 0.00001) context.put(i, coef[coefToUse]);
//				i++;
//			}
//		}

		//System.out.println("Coefs: " + context);

		double denom = 0;

		// The first coefficient is the intercept, so we ignore it for the
		// normalizing denominator: sqrt(a^2 + b^2 + ... + q^2).

		for (int i = 1; i < context.length; i++) {
			denom += context.get(i)*context.get(i);
		}

		denom = Math.sqrt(denom);

		//if (denom < 0.01) denom = 0.01;

		for (int i = 0; i < context.length; i++) {
			context.put(i, context.get(i)/denom);
		}

		lastContext = context;

		return context;
	}



	public double getUncertainty(int itemId, ArrayList<Double> featVals) {
		double value = 0;

		if (cModel == null) return value;

		try {
			Instance inst = createInstance(itemId, featVals);
			double[] classProbs;

			classProbs = cModel.distributionForInstance(inst);

			ClassPrediction pred = new ClassPrediction(classProbs, labels);
			value = 1.0 - pred.prob;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return value;
	}

	@Override
	public double getItemUncertainty(int itemId) {


		return getUncertainty(itemId, getItemFeatures(itemId));
	}

	public ArrayList<Double> getItemFeatures(int itemId) {
		ArrayList<Double> features = featureCache.get(itemId);

		if (features == null) {
			double[] udfVals = KeyWordsUDF.getValue(itemId);
			features = new ArrayList<Double>();
			for (double v : udfVals) {
				features.add(v);
			}
			featureCache.put(itemId, features);
		}		

		return features;
	}

	public double getStoppingValue() {
		double stoppingVal = 0.0;
//		if (wekaStopping.size() > 0) {
//			try {
//				Evaluation evals = new Evaluation(wekaStopping);
//				evals.evaluateModel(cModel, wekaStopping);
//
//				FastVector preds = evals.predictions();
//
//				for (int i = 0; i < preds.size(); i++) {
//					ClassPrediction classPred = new ClassPrediction(((NominalPrediction) preds.get(i)).distribution(), labels);
//					//double uncert = classPred.getUncertainty(); //1.0 - classPred.prob;
//					stoppingVal += classPred.getEntropy(); //1.0 - classPred.prob;
//					//double uncert = classPred.getMargin(); //1.0 - classPred.prob;
//				}
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} 
//		}
		return stoppingVal;
	}

	@Override
	public HashMap<Integer, Double> getUncertainties(ArrayList<Integer> itemIds) {
		HashMap<Integer, Double> uncerts = new HashMap<Integer, Double>();

		if (wekaEval.size() == 0) {
			for (int id : itemIds) {
				Instance inst = createInstance(id, getItemFeatures(id));
				wekaEval.add(inst);
				evalIds.add(id);
			}
		}

		try {
			Evaluation evals = new Evaluation(wekaEval);
			evals.evaluateModel(cModel, wekaEval);

			FastVector preds = evals.predictions();

			for (int i = 0; i < preds.size(); i++) {
				ClassPrediction classPred = new ClassPrediction(((NominalPrediction) preds.get(i)).distribution(), labels);
				//double uncert = classPred.getUncertainty(); //1.0 - classPred.prob;
				double uncert = classPred.getEntropy(); //1.0 - classPred.prob;
				//double uncert = classPred.getMargin(); //1.0 - classPred.prob;
				uncerts.put(evalIds.get(i), uncert);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		return uncerts;
	}

	@Override
	public List<Integer> getItemIds() {
		return itemIds;
	}

	@Override
	public int[] getCounts() {
		int[] counts = new int[labels.length];
		for (int i = 0; i < labels.length; i++) {
			counts[i] = labelCounts.get(labels[i]);
		}
		return counts;
	}

	public HashMap<DoubleMatrix, Double> getAltContexts() {
		HashMap<DoubleMatrix, Double> contexts = new HashMap<DoubleMatrix, Double>();

		/*
		int max = Collections.max(stepsSinceSeen.values());

		for (int i = 0; i < labels.length; i++) {
			int steps = stepsSinceSeen.get(labels[i]);

			// Since we're making alternate contexts, don't make the one we
			// were just in (steps == max)
			if (steps != max) {
				DoubleMatrix m = DoubleMatrix.zeros(labels.length);
				m.put(i, 1.0);

				// If the spot in the context matches the one for the label we just got,
				// give it a hypothetical performance of 1.0
				if (labels[i].equals(itemLabel)) {
					contexts.put(m, 1.0);
				}
				else {
					contexts.put(m, 0.0);
				}
			}
		}
		 */
		return contexts;
	}


	// TODO: This doesn't work. Just put it here to get it out of the way
	private double performanceLRU() {

		// We are going to keep track of how long it's been since
		// each label has been seen, since it might be useful for
		// generating the context.
		int numHigher = 0;

		for (int i = 0; i < labels.length; i++) {
			if (stepsSinceSeen.get(labels[i]) > stepsSinceSeen.get(itemLabel)) {
				numHigher++;
			}
		}

		double perf = 1.0 - (double) numHigher/(labels.length - 1);
		/*
		if (numHigher == 0)
			performance = 1.0;
		else
			performance = 0.0;

		for (String lbl : labels) {
			if (lbl.equals(itemLabel)) {
				stepsSinceSeen.put(lbl, 0);
			}
			else {
				stepsSinceSeen.put(lbl, stepsSinceSeen.get(lbl) + 1);
			}
		}
		 */

		return perf;
	}

	private double performanceLabelCounts() {
		ArrayList<Integer> counts = new ArrayList<Integer>();
		int labelId = 0;

		for (int i = 0; i < labels.length; i++) {
			if (labels[i].equals(itemLabel)) labelId = i;

			counts.add(labelCounts.get(labels[i]));
		}

		int min = Collections.min(counts);
		int max = Collections.max(counts);

		double perf = 1.0 - ((double) (counts.get(labelId) - min)/(max - min));

		return perf;
	}

	private double performanceLabelCountsBoolean() {
		int max = Collections.max(labelCounts.values());

		double perf = 0.0;

		if (labelCounts.get(itemLabel) != max) perf = 1.0;

		return perf;
	}

	private double performanceContextDistance() {
		double perf = 0.0;

		DoubleMatrix context = getContext();

		if (lastContext != null) {
			// cosine distance
			perf = 1 - context.dot(lastContext)/(context.norm2() * lastContext.norm2());
		}

		lastContext = context;
		return perf * performanceLabelCountsBoolean();
	}

	private double performanceWronglyClassified() {
		double perf = 0.0;

		// if the predicted label doesn't match the actual label, return 1.0
		// this might encourage it to select items that will likely be wrong
		if (!itemPredictedLabel.equals(itemLabel)) {
			perf = 1.0;
		}

		return perf;
	}

	private double performanceWeightedWronglyClassified() {
		double perf = 0.0;

		// if the predicted label doesn't match the actual label, return 1.0
		// this might encourage it to select items that will likely be wrong
		if (!itemPredictedLabel.equals(itemLabel)) {
			perf = 1.0;
		}

		return 	performanceLabelCounts() * perf;
	}

	private double performanceProbDecisionBoundary() {
		if (itemPrediction != null) {
			double margin = itemPrediction.getMargin();
			return 	1.0 - margin;
		} else {
			return 0.0;
		}
	}

	private double performanceWeightedProbDecisionBoundary() {
		if (itemPrediction != null) {
			double margin = itemPrediction.getMargin();
			return 	performanceLabelCounts() * (1.0 - margin);
		} else {
			return 0.0;
		}
	}
	
	private double performanceProbWronglyClassified() {
		if (itemPrediction != null) {
			if (itemPrediction.isCorrect()) return 1.0 - itemPrediction.prob;
			else return itemPrediction.prob;
		} else {
			return 0.0;
		}
	}
	
	private double performanceAucRoc() {
		double perf = 0.0;
		double newAucRoc = eTest.areaUnderROC(1);

		//if (newAucRoc < aucRoc) performance = 1.0;

		if (newAucRoc < aucRoc) {
			perf = Math.log(aucRoc - newAucRoc);
		} else if (aucRoc < newAucRoc) {
			perf = 0.0 - Math.log(newAucRoc - aucRoc);
		} else {
			perf = 0.0;
		}


		aucRoc = newAucRoc;

		return perf;
	}

	private double performanceDecisionBoundary(ArrayList<Double> featureValues) {		
		if (!contextType.equals("decisionBoundary")) {
			lastContext = getDecisionBoundaryContext(); 
		}
		ArrayList<Double> withIntercept = new ArrayList<Double>(featureValues);
		withIntercept.add(0, 1.0);

		DoubleMatrix observations = new DoubleMatrix(withIntercept);

		double d = Math.abs(lastContext.transpose().mmul(observations).scalar());

		return 1/(1 + d);
	}

	private double performanceWeightedDecisionBoundary(ArrayList<Double> featureValues) {		
		if (!contextType.equals("decisionBoundary")) {
			lastContext = getDecisionBoundaryContext(); 
		}
		ArrayList<Double> withIntercept = new ArrayList<Double>(featureValues);
		withIntercept.add(0, 1.0);

		DoubleMatrix observations = new DoubleMatrix(withIntercept);

		double d = Math.abs(lastContext.transpose().mmul(observations).scalar());

		return performanceLabelCounts() *  1/(1 + d);
	}

	private double performancePctCorrect() {
		return (eTest.pctCorrect() - pctCorrect)/100.0;
	}

	private double performanceConstant() {
		return 1.0;
	}
	
	private double performanceRoundRobin() {
		return 0.0;
	}

	@Override
	public void registerUDFs(ArrayList<UDF> udfList) {
		this.udfList = udfList;
	}

	@Override
	public String getContextType() {
		return contextType;
	}

	@Override
	public String getRewardType() {
		return rewardType;
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
		featureWriter.close();
	}

	@Override
	public void setFilePrefix(String prefix) {
		// TODO Auto-generated method stub

	}

	@Override
	public Double updateReward(int itemId) {
		if (contextType.equals("decisionBoundary") && featureCache.containsKey(itemId)) {
			return performanceDecisionBoundary(featureCache.get(itemId));
		}
		else if (rewardType.equals("wronglyClassified") && featureCache.containsKey(itemId)) {
			double prediction = 1;
			try {
				prediction = eTest.evaluateModelOnce(cModel, createInstance(itemId, featureCache.get(itemId)));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String predictedLabel = labels[(int) prediction];
			String actualLabel = labeler.getLabel(itemId);
			if (predictedLabel.equals(actualLabel)) 
				return 0.0;
			else
				return 1.0;
		}
		else {
			return null;
		}
	}

}
