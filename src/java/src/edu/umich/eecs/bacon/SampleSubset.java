package edu.umich.eecs.bacon;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/****************************************************************
 * Select a random sample from a data set.
 ***************************************************************/
public class SampleSubset {


	/**
	 * The map class of WordCount.
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		private static double pctToKeep;
		private static Random rand;
		private static int numBuckets;

		/**
		 */
		static {
			pctToKeep = 0.05;
			rand = new Random();
			numBuckets = 500;
		}

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + "\t" + value.toString());

			IntWritable outKey = new IntWritable();
			outKey.set(rand.nextInt(numBuckets));
			
			if (rand.nextDouble() <= pctToKeep) {
				output.collect(outKey, value);
			}   	
		}
	}


	/**
	 */
	public static void main(String args []) throws Exception {
		JobConf conf = new JobConf(SampleSubset.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// output compression
		//TextOutputFormat.setCompressOutput(conf, true);
		//TextOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);

		conf.set("mapred.child.java.opts", "-Xmx1000M");

		conf.setMapperClass(Map.class);
		conf.setNumReduceTasks(0);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}



