/**
 * 
 */
package edu.umich.eecs.featext.UDFs_old;

import java.io.IOException;
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
public class InEuropeUDF implements UDF {
  public static final String ID_STRING = "in_europe";
  String udfDesc;
  
  public static UDF createUDF(String udfDesc) {
    return new InEuropeUDF(udfDesc);
  }

  public InEuropeUDF(String udfDesc) {
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

		String capital = "";
    String text = contents.toString();

    int value = searchText(text);
    
    String schemaDescription = " {    \n"
        + " \"name\": \"InEuropeUDFOutput\", \n"
        + " \"type\": \"record\",\n" + " \"fields\": [\n"
        + "   {\"name\": \"value\", \"type\": \"int\"} ]\n" + "}";

    Schema.Parser parser = new Schema.Parser();
		Schema s = parser.parse(schemaDescription);
		
		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("value", value);
    
    task.provideData(key, contents, output);
	}

	public static int searchText(String text) {
		 // First find the capital
    String pat = "(Europe)";
    Pattern p = Pattern.compile(pat);
    Matcher m = p.matcher(text);

    double utility = 0;
    int value = 0;
    while (m.find()) {
        String capital = m.group(1);
        utility = 1;
        value = 1;
        //System.out.println(capital);
    }
    
    return value;
	}
	
	public static int getValue(int id) {
		int val = 0;
		try {
			WikiPage wp = new WikiPage(id);
			val =  searchText(wp.getText());
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
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String getDescription() {
		return ID_STRING;
	}
}
