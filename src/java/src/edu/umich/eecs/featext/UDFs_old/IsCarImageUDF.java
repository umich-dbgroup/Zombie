/**
 * 
 */
package edu.umich.eecs.featext.UDFs_old;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.Tasks.LearningTask;


/**
 * @author Mike Anderson
 *
 */
public class IsCarImageUDF implements UDF {
	public static final String ID_STRING = "is_car_image";
	String udfDesc;

	public static UDF createUDF(String udfDesc) {
		return new IsCarImageUDF(udfDesc);
	}

	public IsCarImageUDF(String udfDesc) {
		this.udfDesc = udfDesc;
	}

	/* (non-Javadoc)
	 * @see edu.umich.eecs.featext.harness.UDF#processBlock(edu.umich.eecs.featext.harness.DataBlock, edu.umich.eecs.featext.harness.LearningTask)
	 */
	@Override
	public void processBlock(DataBlock block, LearningTask task) {
		IntWritable key = new IntWritable();
		BytesWritable contents = new BytesWritable();

		block.get(key, contents);
		
		// Simple hack: if it's in the id range that would put in 
		// the "car_side" directory, it's a car.
		int value = (key.get() >= 3993 && key.get() <= 4115) ? 1 : 0;

		
		String schemaDescription = " {    \n"
				+ " \"name\": \"IsCarImageUDF\", \n"
				+ " \"type\": \"record\",\n" + " \"fields\": [\n"
				+ "   {\"name\": \"value\", \"type\": \"int\"} ]\n" + "}";

		Schema.Parser parser = new Schema.Parser();
		Schema s = parser.parse(schemaDescription);

		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("value", value);

		task.provideData(key, contents, output);
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
