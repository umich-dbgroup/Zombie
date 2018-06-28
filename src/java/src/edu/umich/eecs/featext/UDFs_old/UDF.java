package edu.umich.eecs.featext.UDFs_old;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.Tasks.LearningTask;


/*****************************************************
 * UDF is the generic interface for user-defined code that
 * processes raw objects and feeds a learning task.
 * 
 ******************************************************/
public interface UDF {
  void processBlock(DataBlock block, LearningTask task);
  GenericRecord createOutput(IntWritable key, Writable contents);
  String getDescription();
}