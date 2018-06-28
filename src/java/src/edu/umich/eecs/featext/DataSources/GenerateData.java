package edu.umich.eecs.featext.DataSources;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import edu.umich.eecs.featext.WikiDecoder;

import java.util.Iterator;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;

import javax.imageio.ImageIO;

/********************************************************************
 * <code>GenerateData</code> is a utility class that transforms
 * a text file into Hadoop's SequenceFile format.  It's handy for testing.
 *
 **********************************************************************/
public class GenerateData {
  public GenerateData() {
  }

  public static void main(String argv[]) throws IOException {
    if (argv.length < 2) {
      System.err.println("Usage: GenerateData <textIn> <seqfileOut>");
      return;
    }
    Configuration conf = new Configuration();
    File csvIn = new File(argv[0]).getCanonicalFile();
    FileSystem fs = FileSystem.get(conf);
    IntWritable key = new IntWritable();
    BytesWritable val = new BytesWritable();
    Path p = new Path(argv[1]);

    BufferedReader in = new BufferedReader(new FileReader(csvIn));
    SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, p, key.getClass(), val.getClass());
    try {
      int i = 0;
      String curLine = null;
      while ((curLine = in.readLine()) != null) {
        curLine = curLine.trim();
    		BufferedImage img = null;
    		try {
    			key.set(++i);
    			img = ImageIO.read(new File("test_data/Caltech101/" + curLine));
    			System.out.println("test_data/Caltech101/" + curLine);
    			ByteArrayOutputStream baos = new ByteArrayOutputStream();
    			ImageIO.write(img, "jpg", baos);
    			baos.flush();
    			byte[] imageInBytes = baos.toByteArray();
    			baos.close();
    			val.set(new BytesWritable(imageInBytes));
    			out.append(key, val);
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
//        WikiPage wp = WikiDecoder.decode(curLine);
//        if (curLine.length() > 0) {
//          key.set(wp.getPageId());
//          val.set(new Text(wp.getTitle()+"\n"+wp.getText()));
//          out.append(key, val);
//          i++;
//        }
      }
    } finally {
      IOUtils.closeStream(out);
    }
  }
}