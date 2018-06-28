package edu.umich.eecs.featext.DataSources;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;

import edu.umich.eecs.featext.index.WikiFileOffsetDict;


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
public class FSDataBlock implements DataBlock {
	//SequenceFile.Reader in;
	//FileSystem fs;
	long offset;
	long blockSize;

	static Configuration conf = new Configuration();
	Path fname;
	private int blockId;
	 SequenceFile.Reader reader;

	/**
	 * Creates a new <code>FSDataBlock</code> instance.
	 */
	public FSDataBlock(Path p, long offset, long blockSize) throws IOException {

		this.offset = offset;
		this.blockSize = blockSize;
		fname = p;
		if (reader == null) {
			reader = getReader();
		}
		reader.seek(offset);
	}

	private SequenceFile.Reader getReader() {
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

	public long getPosition() {
		long pos = -1;
		try {
			pos = reader.getPosition();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return pos;
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

	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();

			FileSystem fs = FileSystem.get(conf);
			Path p = new Path("test_data/wikipedia/wiki.seq");

			SequenceFile.Reader in = new SequenceFile.Reader(fs, p, conf);
			IntWritable key = (IntWritable) in.getKeyClass().newInstance();
			Writable value = (Writable) in.getValueClass().newInstance();
			int i = 0;

			System.out.println(in.getPosition());
			while (in.next(key, value)) {
				System.out.println(in.getPosition());
				System.out.println(key.get());
				System.out.println(value);
				break;
			}
			in.close();

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
	}

	@Override
	public void setBlockId(int blockId) {
		this.blockId = blockId;
	}

	@Override
	public int getBlockId() {
		// TODO Auto-generated method stub
		return blockId;
	}
}
