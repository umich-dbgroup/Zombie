/**
 * 
 */
package edu.umich.eecs.featext.UDFs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Scanner;

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
public class CIACountUDF implements UDF {
	public static final String ID_STRING = "cia_count";

	String udfDesc;

	private long executionTime;
	static QADataSource data = new QADataSource();
	static HashMap<String, String> countryCodes = new HashMap<String, String>();

	public static UDF createUDF(String udfDesc) {
		return new CIACountUDF(udfDesc);
	}

	public CIACountUDF(String udfDesc) {
		this.udfDesc = udfDesc;
		loadCountryCodes();
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

	public static int ciaCount(String query, String lookForToken) {
		int count = 0;

		try {
			// Guess the country ID
			String code = null;
			String[] words = query.replace("?",  "").split("[ ']");
			for (String word : words) {
				if (countryCodes.containsKey(word)) {
					code = countryCodes.get(word).toLowerCase();
				}
			}

			if (code != null) {
				String queryURL = "https://www.cia.gov/library/publications/the-world-factbook/geos/" + code + ".html";
				URL url = new URL(queryURL);
				int hash = queryURL.hashCode();
				String cacheName = "cia"+hash;

				String result = getFromCache(cacheName);

				if (result == null) {
					URLConnection conn = url.openConnection();
					// fake request coming from browser
					conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB; rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)");
					BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
					String line;

					while ((line = in.readLine()) != null) {
						result += line + "\n";
					}
					in.close();

					writeToCache(cacheName, result);
				}

				int lastIndex = 0;
				lookForToken = lookForToken.replace("_", " ").toLowerCase();
				result = result.toLowerCase();
				while(lastIndex != -1){
					lastIndex = result.indexOf(lookForToken,lastIndex);
					if( lastIndex != -1){
						count ++;
						lastIndex+=lookForToken.length();
					}
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

		val[0] = ciaCount(question, answer);
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
		if (parts.length == 2) {
			String question = parts[0].trim();
			String answer = parts[1].trim();

			value[0] = (double) ciaCount(question, answer);
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

	public static void loadCountryCodes() {
		try {
			File file = new File("test_data/countries/country_codes.txt");

			Scanner scanner;
			scanner = new Scanner(file);

			try {
				while(scanner.hasNextLine()) {        
					String[] parts = scanner.nextLine().split("\t");
					countryCodes.put(parts[1], parts[0]);
				}
			} finally {
				scanner.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}


	public static void main(String[] args) {
		String question = "What the capital of Germany?";
		String answer = "Berlin";
		int count = ciaCount(question, answer);
		System.out.println("Count: " + count);
	}

	@Override
	public long getExecutionTime() {
		return executionTime;
	}
}
