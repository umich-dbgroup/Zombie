package edu.umich.eecs.featext.DataSources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;

import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.index.WikiFileOffsetDict;

import java.io.IOException;
import java.util.List;

/********************************************************************
 * The <code>FSDataBlock</code> is an implementation of the DataBlock
 * class that is a simple array of bytes on disk.  The block size is
 * configurable
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 *******************************************************************/
public class ArrayDataBlock implements DataBlock {
	List<String> data;
	Integer idOffset = 1;
	Integer currentId = idOffset;

	static Configuration conf = new Configuration();
	Path fname;

	private int blockId;
	
	/**
	 * Creates a new <code>FSDataBlock</code> instance.
	 */
	public ArrayDataBlock(List<String> data) {
		this.data = data;
	}

	public long getStart() {
		return 0;
	}
	public long getSize() {
		return 0;
	}

	public long getPosition() {
		return 0;
	}

	public boolean next(Writable key, Writable val) {
		return get(key, val);
	}

	public boolean get(Writable key, Writable val) {
		if (currentId <= data.size()) {
			((IntWritable) key).set(currentId);
			((Text) val).set(data.get(currentId - idOffset));
			currentId++;
			
			return true;
		} else {
			return false;
		}

		
	}

	@Override
	public void setBlockId(int blockId) {
		this.blockId = blockId;
	}

	@Override
	public int getBlockId() {
		return blockId;
	}

}
