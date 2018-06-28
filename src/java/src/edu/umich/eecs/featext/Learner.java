package edu.umich.eecs.featext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import weka.classifiers.functions.LibLINEAR;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;
import edu.umich.eecs.featext.DataSources.WikiIndexer;
import edu.umich.eecs.featext.DataSources.WikiPage;
import edu.umich.eecs.featext.index.FeatureDocumentsIndex;
import edu.umich.eecs.featext.index.FeatureNameDict;
import edu.umich.eecs.featext.index.ItemRetrievalException;
import edu.umich.eecs.featext.index.PostingsList;

/*************************************************************
 * <code>Learner</code> does something or other.  I'm sure of it!
 *
 *************************************************************/
public class Learner {
	private UDF udf;
	private int positiveClassSize;
	private List<Integer> trainIds;
	private List<Integer> positiveClass;
	private List<Integer> negativeClass;
	private HashMap<Integer, Integer> featureMap;
	private ArrayList<Integer> featureList;
	private List<Integer> nextIdSet;
	private Instances dataset;
	private HashSet<Integer> seenIds;
	private HashSet<Integer> positiveIds;
	private ArrayList<Integer> remainingIds;
	private long startTime;

	public Learner(UDF udf) {
		this.udf = udf;
		this.trainIds = new ArrayList<Integer>();
		this.positiveClass = new ArrayList<Integer>();
		this.negativeClass = new ArrayList<Integer>();
		this.featureMap = new HashMap<Integer, Integer>();
		this.featureList = new ArrayList<Integer>();
		this.seenIds = new HashSet<Integer>();
		this.positiveIds = new HashSet<Integer>();
		this.remainingIds = new ArrayList<Integer>();
	}

	public void buildTrainingSet(int size) throws IOException, InstantiationException, IllegalAccessException {
		// TODO: this hard-set number is from the index size that just
		// happened to get saved on my laptop
		this.positiveClassSize = size/2;

		this.startTime = System.currentTimeMillis();
		
		List<Integer> pageIds = this.getNextIdSet(5000000);
		this.remainingIds = new ArrayList<Integer>(pageIds);
		
		// Get the training examples. We need some positive ones.
		System.out.println("Building training set...");

		this.positiveClass = new ArrayList<Integer>();
		this.negativeClass = new ArrayList<Integer>();
		
		//Collections.shuffle(pageIds);

		for (Integer id : pageIds) {
			this.testOutput(id);
			
			int trainingClass = this.getTrainingClass(id);
			
			if (trainingClass == 1 && this.positiveClass.size() < size/2) this.positiveClass.add(id);
			else if (trainingClass == 0 && this.negativeClass.size() < size) this.negativeClass.add(id);

			if (this.positiveClass.size() >= size/2 && this.negativeClass.size() >= size/2) break;
		}

		if (this.positiveClass.size() > size/2) {
			this.positiveClass = this.positiveClass.subList(0, size/2);
		}

		if (this.negativeClass.size() > size - this.positiveClass.size()) {
			this.negativeClass = this.negativeClass.subList(0, size - this.positiveClass.size());
		}

		this.trainIds.addAll(this.positiveClass);
		this.trainIds.addAll(this.negativeClass);
			


		System.out.println("Positive: " + this.positiveClass.size() + 
				           ", Negative: " + this.negativeClass.size());
		
		// Go through the training examples and collect all the features.
		for (Integer id : this.trainIds) {
			WikiPage wp = new WikiPage(id);
			HashMap<Integer, Float> features;
			try {
				features = wp.getFeatures();
				for (Map.Entry<Integer, Float> entry : features.entrySet()) {
					int fId = entry.getKey();
					if (this.featureMap.containsKey(fId)) {
						//mappedId = this.featureMap.get(fId);
						this.featureMap.put(fId, this.featureMap.get(fId) + 1);
					}
					else {
						this.featureMap.put(fId, 1);
					}
				}
			} catch (ItemRetrievalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println("Number of features: " + this.featureMap.size());
		
		// Start building the Instances		
		ArrayList<String> labels = new ArrayList<String>();
		labels.add("positive");
		labels.add("negative");
		
		Attribute cls = new Attribute("class", labels);
		
		// Build a list of all features we will use, so we can use the list
		// index as a temporary feature ID.
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		HashMap<Integer, Integer> featureLocs = new HashMap<Integer, Integer>();
		int loc = 0;
		for (Map.Entry<Integer, Integer> entry : this.featureMap.entrySet()) {
			// leave out rare features
			if (entry.getValue() >= this.positiveClass.size()/4) {
				attributes.add(new Attribute(String.valueOf(entry.getKey())));
				featureLocs.put(entry.getKey(), loc++);
				this.featureList.add(entry.getKey());
			}
		}
		attributes.add(cls);
		this.dataset = new Instances("Dataset", attributes, 0);
		this.dataset.setClass(cls);

		for (Integer id : this.positiveClass) {
			WikiPage wp = new WikiPage(id);
			HashMap<Integer, Float> features;
			double[] values = new double[this.dataset.numAttributes()];
			try {
				features = wp.getFeatures();
				for (Map.Entry<Integer, Float> entry : features.entrySet()) {
					if (featureLocs.containsKey(entry.getKey())) {
						values[featureLocs.get(entry.getKey())] = 1.0; // Could use some other value.
					}
				}

				Instance inst = new SparseInstance(1.0, values);
				inst.setDataset(this.dataset);				
				inst.setClassValue("positive");
				dataset.add(inst);
			} catch (ItemRetrievalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		for (Integer id : this.negativeClass) {
			WikiPage wp = new WikiPage(id);
			HashMap<Integer, Float> features;
			double[] values = new double[dataset.numAttributes()];

			try {
				features = wp.getFeatures();
				for (Map.Entry<Integer, Float> entry : features.entrySet()) {
					if (featureLocs.containsKey(entry.getKey())) {
						values[featureLocs.get(entry.getKey())] = 1.0;//entry.getValue(); // Could use some other value.
					}
				}
				Instance inst = new SparseInstance(1.0, values);
				inst.setDataset(dataset);
				inst.setClassValue("negative");
				dataset.add(inst);
			} catch (ItemRetrievalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public String testOutput(Integer docId) throws IOException, InstantiationException, IllegalAccessException {
		String out = null;
		String outputFileName = String.format("test_data/%s_logreg_%d.txt", this.udf.getName(), this.positiveClassSize);
		File file = new File(outputFileName);
		 
		// if file doesn't exist, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
		BufferedWriter bw = new BufferedWriter(fw);
		
		if (!this.seenIds.contains(docId)) {
			this.seenIds.add(docId);
			WikiPage wp = new WikiPage(docId);
			HashMap<String, String> input = new HashMap<String, String>();			
			input.put("text", wp.getText());
			if (udf.test(input)) {
				this.positiveIds.add(docId);
				this.remainingIds.remove(docId);
				long curTime = System.currentTimeMillis();
				//System.out.println(this.seenIds.size()+"\t"+this.positiveIds.size());
				bw.write(this.seenIds.size()+"\t"+this.positiveIds.size()+"\t" + (curTime - this.startTime) + "\n");
			}
		}
		bw.close();
		
		return out;
	}
	
	public void writeNewIdIndex() throws IOException {
		String outputFileName = String.format("test_data/%s_index.txt", this.udf.getName(), this.positiveClass.size());
		File file = new File(outputFileName);
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		for (int docId : this.positiveIds) {
			bw.write(docId + "\n");
		}
		for (int docId : this.remainingIds) {
			bw.write(docId + "\n");
		}
		bw.close();
	}
	
	private List<Integer> getNextIdSet(int amount) {
		if (this.nextIdSet == null) {
			this.nextIdSet = WikiIndexer.getPageIds(amount, 0);
		}
		
		return this.nextIdSet;
	}
	
	public int getTrainingClass(int docId) throws IOException, InstantiationException, IllegalAccessException {
		WikiPage wp = new WikiPage(docId);

		HashMap<String, String> input = new HashMap<String, String>();

		input.put("text", wp.getText());

		if (this.udf.test(input)) return 1;
		else return 0;
	}

	public Map<Integer, Double> logisticRegression() throws Exception {
		System.out.println("Running logistic regression.");
		
		LibLINEAR liblin = new LibLINEAR();
		
		// -S 6 for L1-regularized Logistic Regression
		String[] options = {"-S", "6"};
		
		liblin.setNormalize(false);
		
		liblin.setOptions(options);
		liblin.buildClassifier(this.dataset);

		FeatureNameDict fn = new FeatureNameDict();
		
		//System.out.println("\nSelected Features:\n");
		
		double[] weights = liblin.getModel().getFeatureWeights();
		
		Map<Integer, Double> selectedFeatures = new HashMap<Integer, Double>();
		
		for (int i = 0; i < weights.length; i++) {
			if (weights[i] > 0 && i < this.featureList.size()) {
				selectedFeatures.put(this.featureList.get(i), weights[i]);
			}
		}
		
		Map<Integer, Double> sorted = Learner.sortByValue(selectedFeatures);
	    double maxValue = (Double) (Collections.max(sorted.values())); 

	    // Normalize the values by dividing by the max
		System.out.println("----------");
		for (Map.Entry<Integer, Double> entry : sorted.entrySet()) {
			System.out.println(fn.get(entry.getKey()) + " : " + entry.getValue());
			sorted.put(entry.getKey(), entry.getValue()/maxValue);
		}
		System.out.println("----------");

		return sorted;
	}

	// Let's make this hash sorted by values
	public static <K, V extends Comparable<V>> Map<K, V> sortByValue(final Map<K, V> map) {
	    Comparator<K> valueComparator =  new Comparator<K>() {
	        public int compare(K k1, K k2) {
	            int compare = map.get(k2).compareTo(map.get(k1));
	            if (compare == 0) return 1;
	            else return compare;
	        }
	    };
	    
	    Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
	    sortedByValues.putAll(map);
    
	    return new LinkedHashMap<K,V>(sortedByValues);
	}
	
	public String[] getFeatureNames(int[] ids) {
		String[] names = new String[ids.length];
		FeatureNameDict fn = new FeatureNameDict();
		for (int i = 0; i < ids.length; i++) {
			names[i] = fn.get(ids[i]);
			System.out.println(names[i]);
		}
		return names;
	}
	
	public HashMap<Integer, Double> getRelatedDocuments(Map<Integer, Double> features) throws ItemRetrievalException, IOException, InstantiationException, IllegalAccessException {
		//TODO: put this somewhere (total document count 4186482)
		// int totalDocCount = 4186482;
		

		FeatureDocumentsIndex fdx = new FeatureDocumentsIndex();

		HashMap<Integer, Double> docs = new HashMap<Integer, Double>();
		
		for (Map.Entry<Integer, Double> feat : features.entrySet()) {
			double score = feat.getValue();
			//if (score < 0.5) continue;
			
			PostingsList pl = fdx.get(feat.getKey());
			
			//int docCount = pl.getPostings().size();

			for (Map.Entry<Integer, Float> posting : pl.getPostings().entrySet()) {	
//				// TF-IDF sort of.... (doesn't seem to do much good in minimal testing)
//				//double score = Math.log(posting.getValue() + 1) * Math.log(totalDocCount/docCount) * feat.getValue();
						
				Double docTotalVal = docs.get(posting.getKey());
				docTotalVal = docTotalVal == null ? score : docTotalVal + score;
				docs.put(posting.getKey(), docTotalVal);
			}
		}
		System.out.println("Filtering " + docs.size() + " documents.");

		
		
		//System.out.println("Sorting " + docs.size() + " documents.");
		docs = (HashMap<Integer, Double>) Learner.sortByValue(docs);
		int i = 0;
		System.out.println("----------");
		
		this.nextIdSet = new ArrayList<Integer>();
		
		for (Entry<Integer, Double> entry : docs.entrySet()) {
			this.testOutput(entry.getKey());
			//if (++i > 25) break;
			//WikiPage wp = new WikiPage(entry.getKey());
			//HashMap<String, String> input = new HashMap<String, String>();
			//input.put("text", wp.getText());
			//System.out.println(String.format("%-2d. %-30s : %s", i, wp.getTitle(), this.udf.execute(input).get(0)));
			//this.nextIdSet.add(entry.getKey());
		}
			
		return docs;
	}
	
	public static void main(String[] args) {
		int numTrain = 20;

		try {
//			System.out.println("Country Capital:\n---------------");
//			Learner lrn = new Learner(new ExtractCapitalUDF());
//			lrn.buildTrainingSet(numTrain);
//			Map<Integer, Double> features = lrn.logisticRegression();
//			lrn.getRelatedDocuments(features);

			
//			System.out.println("\nPlant Genus:\n---------------");
			Learner lrn = new Learner(new PlantGenusUDF());
			lrn.buildTrainingSet(numTrain);
			Map<Integer, Double>features = lrn.logisticRegression();
			lrn.getRelatedDocuments(features);
			lrn.writeNewIdIndex();
//
//			
//			System.out.println("\nBirth Year:\n---------------");
//			lrn = new Learner(new BirthYearUDF());
//			lrn.buildTrainingSet(numTrain);
//			features = lrn.logisticRegression();
//			lrn.getRelatedDocuments(features);

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
