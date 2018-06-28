package edu.umich.eecs.featext.Policies;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.Tasks.LearningTask;


/***********************************************
 * <code>Policy</code> uses the index to decide which
 * block to process next.
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 ***********************************************/
public interface Policy {
  public void init(DataSource data, LearningTask ltask, Path fname);
  public DataBlock getNextDataBlock();
  public double[] getOutput();
}