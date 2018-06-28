package edu.umich.eecs.featext.DataSources;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;

import edu.umich.eecs.featext.index.ImageFileOffsetDict;


import java.io.IOException;

/********************************************************************
 * The <code>FSDataBlock</code> is an implementation of the DataBlock
 * class that is a simple array of bytes on disk.  The block size is
 * configurable
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 *******************************************************************/
public class ImageDataBlock implements DataBlock {
  //SequenceFile.Reader in;
  //FileSystem fs;
  long offset;
  long blockSize;
  
	public static ImageFileOffsetDict offsetDict = new ImageFileOffsetDict();
  static Configuration conf = new Configuration();
	static Path fname = new Path("test_data/Caltech101.seq");

  static SequenceFile.Reader reader = getReader();
  /**
   * Creates a new <code>FSDataBlock</code> instance.
   */
  public ImageDataBlock(Path p, long offset, long blockSize) throws IOException {

    this.offset = offset;
    this.blockSize = blockSize;
    
    reader.seek(offset);
  }

  private static SequenceFile.Reader getReader() {
    FileSystem fs = null;
    SequenceFile.Reader rdr = null;
    try {
		  fs = FileSystem.get(conf);
		  rdr = new SequenceFile.Reader(fs, fname, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    return rdr;
  }
  
  public long getStart() {
    return offset;
  }
  public long getSize() {
    return blockSize;
  }
  public boolean next(Writable key, Writable val) {
    try {
      if (offset + blockSize < reader.getPosition()) {
        return false;
      }
      return reader.next(key, val);
    } catch (IOException iex) {
      return false;
    }
  }
  
  public boolean get(Writable key, Writable val) {
  	boolean result = next(key, val);
  	try {
			reader.seek(offset);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  	return result;
  }

	@Override
	public long getPosition() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setBlockId(int blockId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getBlockId() {
		// TODO Auto-generated method stub
		return 0;
	}

  
}