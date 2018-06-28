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

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.WikiPage;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.harness.ExperimentParameters;


/**
 * @author Mike Anderson
 *
 */
public class WordCountUDF implements UDF {
	public static final String ID_STRING = "word_count";
	String udfDesc;
	private long executionTime = 0;
	private long loadTime = 0;
	AbstractSequenceClassifier<CoreLabel> classifier;

	Random rand = new Random();

	private static List<String> words; 

	public static UDF createUDF(String udfDesc) {
		return new WordCountUDF(udfDesc);
	}

	public WordCountUDF(String udfDesc) {
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

		task.provideData(key, contents, output);
	}

	public  double[] searchText(String text) {
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		HashMap<String, Integer> allCounts = new HashMap<String, Integer>();
		int maxCount = 0;
		
		int wordCount = text.split("\\s+").length;
		
		
		double[] values = new double[1];
		values[0] = (double) wordCount;
		
		return values;
	}

	public double[] getValue(int id) {
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
		String[] featNames = {"wordCount"};
		
		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("execTime", executionTime);
		output.put("featureNames", featNames);
		output.put("value", value);

		return output;
	}

	public static String cleanText(String text) {
		String clean = text;

		clean = clean.replaceAll("[\\*\\]\\[\\{\\}\\|=]+", " ");
		clean = clean.replaceAll("''+", "\"");
		clean = clean.replaceAll("<[^<>]+>", " ");
		clean = clean.replaceAll("\\s+", " ");
		clean = clean.replaceAll("[^\\p{ASCII}]", ""); // remove non-ASCII

		return clean;
	}

	@Override
	public String getDescription() {
		return ID_STRING;
	}

	@Override
	public long getExecutionTime() {
		return executionTime;
	}

//	public static void main(String[] args) {
//		NERUDF udf = new NERUDF("foo");
//		System.out.println(udf.getValue(21241195));
//	}

}
