/**
 * 
 */
package edu.umich.eecs.featext.UDFs_old;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.Tasks.LearningTask;



/**
 * @author Mike Anderson
 *
 */
public class RoughClassifierUDF implements UDF {
	public static final String ID_STRING = "rough_classifier";
	//public static final String[] classNames = {"science", "sports", "politics", "videoGames"};//, "other"};
	public static final String [] classNames = {"geography", "other"};
	
	String udfDesc;

	public static UDF createUDF(String udfDesc) {
		return new RoughClassifierUDF(udfDesc);
	}

	public RoughClassifierUDF(String udfDesc) {
		this.udfDesc = udfDesc;
	}

	/* (non-Javadoc)
	 * @see edu.umich.eecs.featext.harness.UDF#processBlock(edu.umich.eecs.featext.harness.DataBlock, edu.umich.eecs.featext.harness.LearningTask)
	 */
	@Override
	public void processBlock(DataBlock block, LearningTask task) {
		IntWritable key = new IntWritable();
		Writable contents = new Text();
		block.next(key, contents);
		
		GenericRecord output = createOutput(key, contents);

		task.provideData(key, contents, output);
	}

	public GenericRecord createOutput(IntWritable key, Writable contents) {
		String text = contents.toString();
		String classGuess = classify(text);

		String schemaDescription = " {    \n"
				+ " \"name\": \"RoughClassifierUDFOutput\", \n"
				+ " \"type\": \"record\",\n" + " \"fields\": [\n"
				+ "   {\"name\": \"class\", \"type\": \"string\"} ]\n" + "}";

		Schema.Parser parser = new Schema.Parser();
		Schema s = parser.parse(schemaDescription);

		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("class", new org.apache.avro.util.Utf8(classGuess));

		return output;
	}

	public static String classify(String text) {
		String pat = "\\[Category:([^\\r\\n\\[\\|\\]]*)[\\]\\|]";
		Pattern p = Pattern.compile(pat);
		Matcher m = p.matcher(text);

		String[] science = {"biology", "chemistry", "physics", "science", "scientist",
				"meteorology","astronomy", "geology", "genetic"};

		String[] sports = {"baseball", "football", "basketball", "hockey", "soccer",
				"tennis", "golf", "curling", "skiing", "olympics", "sports", 
				"swimming", "athletic", "athlete"};

		String[] politics = {"president", "senate", "congress", "election", "mayor", 
				"governor", "voting", "council", "king", "queen", "parliament"};

		String[] videoGames = {"video game", "arcade game", "windows game", "dos game"};
		
		String[] geography = {"cities in", "counties", "states of", "countries in"};

		String classGuess = "other";

		while (m.find()) {
			String category = "";
			category = m.group(1).toLowerCase();

//			for (String str : science) {
//				Pattern pattern = Pattern.compile(str);
//				Matcher matcher = pattern.matcher(category);
//				if (matcher.find()) {
//					classGuess = "science";
//				}
//			}

//			for (String str : sports) {
//				Pattern pattern = Pattern.compile(str);
//				Matcher matcher = pattern.matcher(category);
//				if (matcher.find()) {
//					classGuess = "sports";
//				}
//			}

//			for (String str : politics) {
//				Pattern pattern = Pattern.compile(str);
//				Matcher matcher = pattern.matcher(category);
//				if (matcher.find()) {
//					classGuess = "politics";
//				}
//			}
			
			for (String str : geography) {
				Pattern pattern = Pattern.compile(str);
				Matcher matcher = pattern.matcher(category);
				if (matcher.find()) {
					classGuess = "geography";
				}
			}

//			for (String str : videoGames) {
//				Pattern pattern = Pattern.compile(str);
//				Matcher matcher = pattern.matcher(category);
//				if (matcher.find()) {
//					classGuess = "videoGames";
//				}
//			}
		}

		return classGuess;
	}

	public static List<Integer> getPageIds() {
		ArrayList<Integer> idList = new ArrayList<Integer>();

		try {
			FileReader fr = new FileReader("goodClassIds.txt");
			BufferedReader br = new BufferedReader(fr);
			String line = "";

			while ((line = br.readLine()) != null) {
					idList.add(new Integer(line));				
			}

			fr.close();
		}
		catch (IOException ex) {
			System.out.println("Could not read from file " + ex);
		}		

		return idList;
	}
	
//	public static void main(String[] args) {
//		try {
//			Path fname = new Path("test_data/wikipedia/wiki.seq");
//			List<Integer> pageIds = RoughClassifierUDF.getPageIds();
//			UDF udf = new RoughClassifierUDF("test");
//			PrintWriter pw = new PrintWriter(new FileOutputStream("goodClassIds2.txt"));
//			for (int id : pageIds) {
//				FileOffset fo = FSDataBlock.offsetDict.get(id);
//				DataBlock block;
//
//				block = (DataBlock) new FSDataBlock(fname, fo.getOffset(), (long) fo.getLength());
//
//				GenericRecord out = udf.createOutput(block);
//				String cls = out.get("class").toString();
//
//				if (cls != "other") {
//					pw.println(id);
//				}
//			}
//			pw.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	@Override
	public String getDescription() {
		return ID_STRING;
	}
	
}
