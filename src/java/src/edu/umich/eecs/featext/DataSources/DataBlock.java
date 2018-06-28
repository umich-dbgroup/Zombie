package edu.umich.eecs.featext.DataSources;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**************************************************
 * DataBlock is a region of raw data that the user's
 * UDF will process.  The object here is meant to describe
 * the block, not actuall contain the raw bytes.  By calling
 * getIterator(), the user of this object can iterate over
 * the actual on-disk information.
 *
 **************************************************/
public interface DataBlock {
	public long getStart();
	public long getSize();
	public long getPosition();
	public boolean next(Writable key, Writable val);
	public boolean get(Writable key, Writable contents);
	public void setBlockId(int blockId);
	public int getBlockId();
}