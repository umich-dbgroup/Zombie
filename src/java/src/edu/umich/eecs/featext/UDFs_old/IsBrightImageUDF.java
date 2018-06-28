/**
 * 
 */
package edu.umich.eecs.featext.UDFs_old;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;

import javax.imageio.ImageIO;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.ImageDataBlock;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.index.FileOffset;

/**
 * @author Mike Anderson
 *
 */
public class IsBrightImageUDF implements UDF {
	public static final String ID_STRING = "is_bright_image";
	String udfDesc;

	public static UDF createUDF(String udfDesc) {
		return new IsBrightImageUDF(udfDesc);
	}

	public IsBrightImageUDF(String udfDesc) {
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
		int value = 0;
		try {
			InputStream in = new ByteArrayInputStream(contents.getBytes());
			BufferedImage img = ImageIO.read(in);

			double bright = getBrightness(img);

			// Consider it blue if the percentage is high enough.
			value = bright > 220 ? 1 : 0;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String schemaDescription = " {    \n"
				+ " \"name\": \"IsBrightImageUDF\", \n"
				+ " \"type\": \"record\",\n" + " \"fields\": [\n"
				+ "   {\"name\": \"value\", \"type\": \"int\"} ]\n" + "}";

		Schema.Parser parser = new Schema.Parser();
		Schema s = parser.parse(schemaDescription);

		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("value", value);

		task.provideData(key, contents, output);
	}

	public static double getBrightness(BufferedImage img) {
		int blueCnt = 0;
		int numPixels = img.getWidth() * img.getHeight();
		int sum = 0;

		for (int w = 0; w < img.getWidth(); w++) {
			for (int h = 0; h < img.getHeight(); h++) {
				int rgb = img.getRGB(w, h);
				int red = (rgb >> 16) & 0xFF;
				int green = (rgb >> 8) & 0xFF;
				int blue = (rgb >> 0) & 0xFF;
				
				
				ArrayList<Integer> colorList = new ArrayList<Integer>();
				colorList.add(red);
				colorList.add(green);
				colorList.add(blue);
				sum = sum + Collections.max(colorList);
			}
		}

		return (double) sum/numPixels;
	}

	public static int getValue(int id) {
		Path fname = new Path("test_data/Caltech101.seq");

		FileOffset fo = ImageDataBlock.offsetDict.get(id);
		int val = 0;
		
		try {
			DataBlock block = (DataBlock) new ImageDataBlock(fname, fo.getOffset(), (long) fo.getLength());
			IntWritable key = new IntWritable();
			BytesWritable contents = new BytesWritable();
			block.get(key, contents);

			
			InputStream in = new ByteArrayInputStream(contents.getBytes());
			BufferedImage img = ImageIO.read(in);
			
			double bright = getBrightness(img);

			// Consider it blue if the percentage is high enough.
			val = bright > 220 ? 1 : 0;
		} catch (IOException e) {
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

	public static void main(String[] args) {
		String fname = "test_data/Caltech101/Leopards/image_0038.jpg";
		System.out.println(fname);

		BufferedImage img = null;
		try {
			img = ImageIO.read(new File(fname));
			System.out.println(IsBrightImageUDF.getBrightness(img));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getDescription() {
		return ID_STRING;
	}
}
