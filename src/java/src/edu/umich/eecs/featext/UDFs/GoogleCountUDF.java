/**
 * 
 */
package edu.umich.eecs.featext.UDFs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.QADataSource;
import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.Tasks.LearningTask;


/**
 * @author Mike Anderson
 *
 */
public class GoogleCountUDF implements UDF {
	public static final String ID_STRING = "google_count";

	String udfDesc;

	private long executionTime;
	static QADataSource data = new QADataSource();

	public static UDF createUDF(String udfDesc) {
		return new GoogleCountUDF(udfDesc);
	}

	public GoogleCountUDF(String udfDesc) {
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

		long startTime = System.nanoTime();
		GenericRecord output = createOutput(key, contents);
		long endTime = System.nanoTime();
		executionTime = endTime - startTime;
		
		task.provideData(key, contents, output);
	}

	public static int googleCount(String query, String lookForToken) {
		int count = 0;

		try {
			String queryURL = "http://www.google.com/search?q=" + URLEncoder.encode(query, "UTF-8");
			URL url = new URL(queryURL);
			int hash = queryURL.hashCode();
			String cacheName = "goog"+hash;
			//String cacheFile = queryURL.replaceAll("[^A-Za-z0-9]", "");

			String googleResult = getFromCache(cacheName);

			if (googleResult == null) {

				URLConnection conn = url.openConnection();
				// fake request coming from browser
				conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.5 Safari/537.36");
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
				String line;

				while ((line = in.readLine()) != null) {
					googleResult += line + "\n";
				}
				in.close();

				writeToCache(cacheName, googleResult);
			}

			//FileUtils.copyFile(new File("qa_cache/" + cacheName), new File("qa_cache/" + cacheFile));

			int lastIndex = 0;
			lookForToken = lookForToken.replace("_", " ").toLowerCase();
			googleResult = googleResult.toLowerCase();

			while(lastIndex != -1){
				lastIndex = googleResult.indexOf(lookForToken,lastIndex);
				if( lastIndex != -1){
					count ++;
					lastIndex+=lookForToken.length();
				}
			}
			//System.out.println("Query: " + query + " -- Answer: " + lookForToken + " -- Count: " + count);

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return count;
	}

	public static String getFromCache(String cacheName) {
		String contents = null;
		try {
			File file = new File("qa_cache/" + cacheName);
			contents = Files.toString(file, Charsets.UTF_8);
		} catch (Exception e) {
			// Comment this out, since if it doesn't exist we need to retrieve it.
			//e.printStackTrace();
		}
		return contents;
	}

	public static void writeToCache(String cacheName, String content) {
		try {
			FileUtils.writeStringToFile(new File("qa_cache/" + cacheName), content);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static double[] getValue(int id) {
		double[] val = new double[1];

		Item item = data.getItem(id);
		Writable contents = item.getContents();
		String[] parts = contents.toString().split("\n");
		String question = parts[0].trim();
		String answer = parts[1].trim();

		val[0] = googleCount(question, answer);
		return val;
	}

	@Override
	public GenericRecord createOutput(IntWritable key, Writable contents) {
		String text = contents.toString();

		// return value is array of doubles
		double[] value = new double[1];
		String[] featureNames = new String[1];
		featureNames[0] = "count";

		// The question and answer should be separated by a newline
		String[] parts = text.split("\n");



		// If it's not 2, it's probably a test run by the learner to get
		// the schema. This sounds hacky.
		if (parts.length == 2) {
			String question = parts[0].trim();
			String answer = parts[1].trim();


			value[0] = (double) googleCount(question, answer);
		}

		String schemaDescription = " {    \n"
				+ " \"name\": \"" + getDescription() + "\", \n"
				+ " \"type\": \"record\",\n" + " \"fields\": [\n"
				+ "   {\"name\": \"featureNames\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}, "
				+ "   {\"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"double\"}} "
				+ "]\n" + "}";

		Schema.Parser parser = new Schema.Parser();
		Schema s = parser.parse(schemaDescription);

		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("featureNames", featureNames);
		output.put("value", value);

		return output;
	}

	@Override
	public String getDescription() {
		return ID_STRING;
	}



	public static void main(String[] args) {
		String question = "What the capital of Germany?";
		String answer = "Berlin";
		int count = googleCount(question, answer);
		System.out.println("Count: " + count);
	}

	@Override
	public long getExecutionTime() {
		return executionTime;
	}
}
