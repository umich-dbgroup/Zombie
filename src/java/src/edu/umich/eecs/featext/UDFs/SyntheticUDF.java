/**
 * 
 */
package edu.umich.eecs.featext.UDFs;

import java.io.IOException;
import java.util.HashMap;

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
public class SyntheticUDF implements UDF {
	public static final String ID_STRING = "synth_udf";
	String udfDesc;
	private long executionTime;
	
	public static String[] words;

	private String[] words00 = {
			"c0_3","c0_4","c0_5","c0_6","c0_7", "c0_8",
			"g_199", "g_104","g_105","g_106","g_107", "g_108",
		};	
	
	private String[] words0 = {
			"c0_13","c0_14","c0_15","c0_26","c0_27", "c0_28",
			"g_399", "g_304","g_305","g_306","g_307", "g_308",
			"g_299", "g_204","g_205","g_206","g_207", "g_208",
			};	
	
	private String[] words1 = {
		"c0_23","c0_24","c0_25","c0_26","c0_27", "c0_28",
		"g_199", "g_104","g_105","g_106","g_107", "g_108",
	};	

	private String[] words2 = {
		"c0_33","c0_34","c0_35","c0_36","c0_37", "c0_38",
		"g_399", "g_304","g_305","g_306","g_307", "g_308",
		"g_299", "g_204","g_205","g_206","g_207", "g_208",
	};
	
	private String[] words3 = {
		"c0_49","c0_50","c0_45","c0_46","c0_47", "c0_48",
		"g_199", "g_104","g_105","g_106","g_107", "g_108",
	};


	public static UDF createUDF(String udfDesc) {
		return new SyntheticUDF(udfDesc);
	}

	public SyntheticUDF(String udfDesc) {
		this.udfDesc = udfDesc;
		words = words0;
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
