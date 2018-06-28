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
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import weka.core.Stopwords;

import com.google.common.base.Joiner;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.ItemDataBlock;
import edu.umich.eecs.featext.DataSources.Labeler;
import edu.umich.eecs.featext.DataSources.DemoWikiLabeler;
import edu.umich.eecs.featext.DataSources.WikiPage;
import edu.umich.eecs.featext.Tasks.LearningTask;


/**
 * @author Mike Anderson
 *
 */
public class LabelerUDF implements UDF {
	public static final String ID_STRING = "labeler";
	String udfDesc;
	private long executionTime = 0;
	private long loadTime = 0;
	private static Labeler labeler;
	private static Stopwords stopwords = new Stopwords();

	private static List<String> words; 

	public static UDF createUDF(String udfDesc, Labeler labeler) {
		return new LabelerUDF(udfDesc, labeler);
	}

	public LabelerUDF(String udfDesc, Labeler labeler) {
		this.udfDesc = udfDesc;
		this.labeler = labeler;
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

	public static double[] searchText(String text) {
		long t1 = System.nanoTime();
		String[] labels = labeler.getLabels();
		String itemLabel = ((DemoWikiLabeler) labeler).getLabel(text);
		
		double[] values = new double[labels.length + 1];
		for (int i = 0; i < labels.length; i++) {
			if (itemLabel.equals(labels[i])) values[i] = 1.0;
			else values[i] = 0.0;
		}

		values[values.length - 1] = 0;

		for (String token : text.split("\\s")) {
			if (!Stopwords.isStopword(token)) values[values.length - 1]++;
		}
		
	    try {
			TimeUnit.MILLISECONDS.sleep(10);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		long t2 = System.nanoTime();
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
		output.put("featureNames", labeler.getLabels());
		output.put("value", value);

		return output;
	}

	// Load the words from the dictionary file
	public static List<String> getWords() {
		List<String> lines = new ArrayList<String>();

		try {
			Scanner sc = new Scanner(new File("udf_dict.txt"));

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
