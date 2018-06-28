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
public class IsBlueImageUDF implements UDF {
	public static final String ID_STRING = "is_blue_image";
	String udfDesc;

	public static UDF createUDF(String udfDesc) {
		return new IsBlueImageUDF(udfDesc);
	}

	public IsBlueImageUDF(String udfDesc) {
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

			double pct = getBluePct(img);

			// Consider it blue if the percentage is high enough.
			value = pct > 0.5 ? 1 : 0;
			//value = (int) (100 * pct);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String schemaDescription = " {    \n"
				+ " \"name\": \"IsBlueImageUDF\", \n"
				+ " \"type\": \"record\",\n" + " \"fields\": [\n"
				+ "   {\"name\": \"value\", \"type\": \"int\"} ]\n" + "}";

		Schema.Parser parser = new Schema.Parser();
		Schema s = parser.parse(schemaDescription);

		// Populate data
		GenericRecord output = new GenericData.Record(s);
		output.put("value", value);

		task.provideData(key, contents, output);
	}

	public static double getBluePct(BufferedImage img) {
		int blueCnt = 0;
		int numPixels = img.getWidth() * img.getHeight();

		for (int w = 0; w < img.getWidth(); w++) {
			for (int h = 0; h < img.getHeight(); h++) {
				int rgb = img.getRGB(w, h);
				int red = (rgb >> 16) & 0xFF;
				int green = (rgb >> 8) & 0xFF;
				int blue = (rgb >> 0) & 0xFF;

				// Below 50 is pretty much black. We'll say it's blue if blue is greater
				// than red and green and red and green are pretty close to each other.
				if (blue > 50 && blue > red && blue > green && red < 0.98 * blue && green < 0.98 * blue && 
						((double) red/green > 0.667 && (double) red/green < 1.5)) {
					blueCnt++;
				}
			}
		}

		return (double) blueCnt/numPixels;
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
			
			double pct = getBluePct(img);

			// Consider it blue if the percentage is high enough.
			val = pct > 0.5 ? 1 : 0;
			val = (int) (100 * pct);
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
			System.out.println(IsBlueImageUDF.getBluePct(img));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getDescription() {
		return ID_STRING;
	}
}
