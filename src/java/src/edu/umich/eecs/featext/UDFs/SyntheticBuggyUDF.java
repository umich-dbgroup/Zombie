/**
 * 
 */
package edu.umich.eecs.featext.UDFs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.WikiPage;
import edu.umich.eecs.featext.Tasks.LearningTask;


/**
 * @author Mike Anderson
 *
 */
public class SyntheticBuggyUDF implements UDF {
	public static final String ID_STRING = "synth_udf";
	String udfDesc;
	private long executionTime;
	
	private static double bugRate = 0.01; // out of 1.0
	
	private static Random rand = new Random();
	
	public static String[] words = {
		"c0_25","c0_26","c0_22","c0_23",
		"c1_25","c1_26","c1_22","c1_23",
		"g_100", "g_200",

		};
		//"cars", "book", "music", "author", "actor"};
		// "science", "experiment", "biology", "chemistry", "physics"}; 

	public static UDF createUDF(String udfDesc) {
		return new SyntheticBuggyUDF(udfDesc);
	}

	public SyntheticBuggyUDF(String udfDesc) {
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

		GenericRecord output = createOutput(key, contents);

		task.provideData(key, contents, output);
	}

	public static double[] searchText(String text) {
	
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		for (String s : words) {
			counts.put(s, 0);
//	        Pattern pattern = Pattern.compile(s);
//	        Matcher matcher = pattern.matcher(text);
//
//	        int count = 0;
//	        while (matcher.find())
//	            count++;
//	        
//	        counts.put(s, count);
		}

		for (String w : text.split("[^_0-9a-zA-Z]")) {
			if (counts.containsKey(w)) {
				counts.put(w, counts.get(w) + 1);
			}
		}
		
		double[] values = new double[words.length];
		for (int i = 0; i < words.length; i++) {
			values[i] = counts.get(words[i]);
			if (rand.nextDouble() <= bugRate) {
				values[i] = rand.nextInt(100);
			}
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
		executionTime = endTime - startTime;

		String schemaDescription = " {    \n"
				+ " \"name\": \"SyntheticUDF\", \n"
				+ " \"type\": \"record\",\n" + " \"fields\": [\n"
				+ "   {\"name\": \"execTime\", \"type\": \"long\"}," 
				+ "   {\"name\": \"featureNames\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}, "
				+ "   {\"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"double\"}} ]\n" + "}";

		Schema.Parser parser = new Schema.Parser();
		Schema s = parser.parse(schemaDescription);

		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("execTime", executionTime);
		output.put("featureNames", words);
		output.put("value", value);

		return output;
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
