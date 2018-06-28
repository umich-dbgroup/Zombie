/**
 * 
 */
package edu.umich.eecs.featext.UDFs;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class SyntheticSparseFeatureUDF implements UDF {
	public static final String ID_STRING = "synth_udf";
	String udfDesc;
	private long executionTime;
	public static String[] words = {
		"c0_9[0-9][0-9]",
		"c1_9[0-9][0-9]",		
		"c0_8[0-9][0-9]",
		"c1_8[0-9][0-9]",
		"c0_7[0-9][0-9]",
		"c1_7[0-9][0-9]",
		};
		//"cars", "book", "music", "author", "actor"};
		// "science", "experiment", "biology", "chemistry", "physics"}; 

	public static UDF createUDF(String udfDesc) {
		return new SyntheticSparseFeatureUDF(udfDesc);
	}

	public SyntheticSparseFeatureUDF(String udfDesc) {
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
	        Pattern pattern = Pattern.compile(s);
	        Matcher matcher = pattern.matcher(text);

	        int count = 0;
	        while (matcher.find())
	            count++;
	        
	        counts.put(s, 1); //count);
		}
		
		double[] values = new double[words.length];
		for (int i = 0; i < words.length; i++) {
			values[i] = counts.get(words[i]);
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
