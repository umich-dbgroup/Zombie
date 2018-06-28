package edu.umich.eecs.featext.UDFs_old;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.Tasks.LearningTask;


/*******************************************************
 * <code>GenericUDF</code> is an empty UDF.  It just scans the input
 *
 *******************************************************/
public class GenericUDF implements UDF {
  public static final String ID_STRING = "generic";
  
  public static UDF createUDF(String udfDesc) {
    return new GenericUDF();
  }

  public GenericUDF() {
  }
  
  /**
   * <code>processBlock</code> is an empty UDF that simply iterates over
   * the input and emits nothing.  That is, it makes no calls to the LearningTask
   * to provide any data.
   */
  public void processBlock(DataBlock block, LearningTask task) {
    //for (Iterator<Writable> it = block.getIterator(); it.hasNext(); ) {
    //it.next();
    //}
    // That's it!  DONE!
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