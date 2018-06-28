/**
 * 
 */
package edu.umich.eecs.featext.UDFs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.WikiPage;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.harness.ExperimentParameters;


/**
 * @author Mike Anderson
 *
 */
public class KeyWordsUDF implements UDF {
	public static final String ID_STRING = "key_words";
	String udfDesc;
	private long executionTime = 0;
	private long loadTime = 0;

	Random rand = new Random();
	
	private static List<String> words; 

	public static UDF createUDF(String udfDesc) {
		return new KeyWordsUDF(udfDesc);
	}

	public KeyWordsUDF(String udfDesc) {
		this.udfDesc = udfDesc;
	}

	/* (non-Javadoc)
	 * @see edu.umich.eecs.featext.harness.UDF#processBlock(edu.umich.eecs.featext.harness.DataBlock, edu.umich.eecs.featext.harness.LearningTask)
	 */
	@Override
	public void processBlock(DataBlock block, LearningTask task) {
		IntWritable key = new IntWritable();
		Writable contents = new Text();

		block.get(key, contents);

		loadTime = ((ItemDataBlock) block).getItem().getLoadTime();

		GenericRecord output = createOutput(key, contents);

		if (((ItemDataBlock) block).getItem().shouldRandomizeFeatures()) {
			double[] vals = (double[]) output.get("value");
			// pick an element. if it's > 0, make it zero, else, make it [1,5]
			for (int i = 0; i < vals.length; i++) {
				if (ExperimentParameters.check("noiseType", "random")) {
					vals[i] = rand.nextInt(2);
				}
				else if (ExperimentParameters.check("noiseType", "zeros")) {
					vals[i] = 0.0; 

				}
				else if (ExperimentParameters.check("noiseType",  "ones")) {
					vals[i] = 1.0;
				}
				else if (ExperimentParameters.check("noiseType", "opposite")) {
					if (vals[i] > 0) vals[i] = 0.0;
					else vals[i] = rand.nextInt(5) + 1;
				}
			}

			output.put("value", vals);
		}
		
		task.provideData(key, contents, output);
	}

	public static double[] searchText(String text) {
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		HashMap<String, Integer> allCounts = new HashMap<String, Integer>();
		int maxCount = 0;

		words = getWords();
		
		for (String s : words) {
			counts.put(s.toLowerCase(), 0);
		}

		for (String token : text.split("\\W")) {
			String w = token.toLowerCase();
			if (counts.containsKey(w)) {
				counts.put(w, counts.get(w) + 1);
			}
			
			if (allCounts.containsKey(w)) {
				allCounts.put(w, allCounts.get(w) + 1);
			}
			else {
				allCounts.put(w, 1);				
			}
			
			if (allCounts.get(w) > maxCount) {
				maxCount = allCounts.get(w);
			}
		}
		

		double[] values = new double[words.size()];
		for (int i = 0; i < words.size(); i++) {
			values[i] = counts.get(words.get(i));
			//values[i] = 0.5 + (0.5 * (double) counts.get(words.get(i)))/((double) maxCount);
		}

		return values;
	}

	public static double[] getValue(int id) {
		double[] val = null;

		try {
			WikiPage wp = new WikiPage(id);
			val = searchText(wp.getText());
			Double[] temp = new Double[val.length];
			for (int i = 0; i < val.length; i++) {
				temp[i] = val[i];
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return val;
	}

	@Override
	public GenericRecord createOutput(IntWritable key, Writable contents) {
		long startTime = System.nanoTime();

		String text = contents.toString();
		double[] value = searchText(text);

		long endTime = System.nanoTime();
		executionTime = endTime - startTime + loadTime;

		String schemaDescription = " {    \n"
				+ " \"name\": \"KeyWordsUDF\", \n"
				+ " \"type\": \"record\",\n" + " \"fields\": [\n"
				+ "   {\"name\": \"execTime\", \"type\": \"long\"}," 
				+ "   {\"name\": \"featureNames\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}, "
				+ "   {\"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"double\"}} ]\n" + "}";

		Schema.Parser parser = new Schema.Parser();
		Schema s = parser.parse(schemaDescription);

		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("execTime", executionTime);
		output.put("featureNames", words.toArray(new String[0]));
		output.put("value", value);

		return output;
	}

	// Load the words from the dictionary file
	public static List<String> getWords() {
		List<String> lines = new ArrayList<String>();

		try {
            String udfDictFile = "udf_dict.txt";
            if (ExperimentParameters.check("numClasses", "2"))
                udfDictFile = "udf_dict.txt.2class";
            else if (ExperimentParameters.check("numClasses", "6"))
                udfDictFile = "udf_dict.txt.6class";

			Scanner sc = new Scanner(new File(udfDictFile));

			while (sc.hasNextLine()) {
				lines.add(sc.nextLine().trim());
			}
			
			sc.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lines;
	}

	@Override
	public String getDescription() {
		return ID_STRING;
	}

	@Override
	public long getExecutionTime() {
		return executionTime;
	}

}
