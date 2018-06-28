package edu.umich.eecs.featext.harness;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.google.common.primitives.Doubles;

import edu.umich.eecs.featext.UDFFeatures;
import edu.umich.eecs.featext.DataSources.DataBlock;
import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.UDFs.UDF;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/*********************************************************
 * <code>TestHarness</code> is the basic loop for building
 * a trained system as quickly as possible.
 *
 * It takes as input various moving parts: a UDF, a Policy,
 * an index, and a LearningTask.  No matter what the inputs,
 * it goes through the look and gathers statistics.
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 *********************************************************/
public class TestHarness {
	DataSource dataSource;
	ArrayList<UDF> udfList;
	Policy policy;
	LearningTask ltask;
	Path fname;
	
	Double aoc;

	/**
	 * A new test requires three things:
	 * 1) A UDF
	 * 2) A block-fetching Policy (which probably contains an index)
	 * 3) A LearningTask
	 */
	public TestHarness(DataSource data, ArrayList<UDF> udfList, Policy policy, LearningTask ltask, Path fname) {
		this.dataSource = data;
		this.udfList = udfList;
		this.policy = policy;
		this.ltask = ltask;
		this.fname = fname;
		
		this.aoc = 0.0;
	}

	/**
	 * Repeatedly grab, process, and exploit blocks for the LearningTask.
	 *
	 * Exit when the system obtains a given level of performance, or a given
	 * runtime has been exceeded.
	 * @throws FileNotFoundException 
	 */
	public void test(double minPerformance, long maxRuntime, int numToProcess, String testName) throws FileNotFoundException {
		long totalStart = System.currentTimeMillis();
		int numBlocks = 0;

		ArrayList<Long> chooseTimes = new ArrayList<Long>();
		
		policy.init(dataSource, ltask, fname);
		System.out.println("Blks\tChoose\tProc\tPerf");
		System.out.println(numBlocks + "\t" + 0 + "\t" + 0 + "\t" + 0);		

		ltask.setFilePrefix("test_data/output/" + testName + ".");
		
		PrintWriter pw = new PrintWriter(new FileOutputStream("test_data/output/" + testName + ".txt"));

		// time to select a block and execute the UDF (leaves out learner time)
		long ourTimeSoFar = 0;
		long totalChooseTime = 0;
		long totalUDFTime = 0;
		long totalLabelTime = 0;
		long totalTrainingTime = 0;
		aoc = 0.0;
		
		while (true) {
			// 1.  Fetch the next data block
			long startChoosingBlock = System.nanoTime();
			DataBlock block = policy.getNextDataBlock();
			if (block == null) {
				break;
			}
			long endChoosingBlock = System.nanoTime();
			long chooseTime = endChoosingBlock - startChoosingBlock;
			chooseTimes.add(chooseTime);
			ourTimeSoFar += chooseTime;
			if (numBlocks > 0) totalChooseTime += chooseTime; // Skip first turn, might be abnormally long for init
			
			// 2.  Process the data block
			long startProcessingBlock = System.nanoTime();
			long udfTime = 0;
			for(UDF udf : udfList) {
				udf.processBlock(block, ltask);
				udfTime += udf.getExecutionTime();
			}
			
			
			long labelTime = ltask.getLabelTime();
			long trainingTime = ltask.getTrainingTime();
			
			totalUDFTime += udfTime;
			totalLabelTime += labelTime;
			totalTrainingTime += trainingTime;
			
			ourTimeSoFar += udfTime + labelTime;
			
			long endProcessingBlock = System.nanoTime();
			long processingTime = endProcessingBlock - startProcessingBlock;
		
			// 3.  Update statistics
			double performance = ltask.getPerformance();
			double[] results = ltask.getResults();
			double[] policyOutput = policy.getOutput();

			aoc += results[0];
			
			long runtimeSoFar = System.currentTimeMillis() - totalStart;			
			numBlocks++;

			// 4.  Log info

			if (numBlocks % 500 == 0) {
				System.out.println(String.format("%d\t%d\t%d\t%d\t%d\t%d\t% .3f\t%f", numBlocks, chooseTime, udfTime, processingTime, labelTime, trainingTime, performance, results[0])); 
			}

			pw.println(((float)runtimeSoFar/1000) + "\t" + numBlocks + "\t" + 
					   ((float)ourTimeSoFar/1000000) + "\t" +
			           //Doubles.join("\t", results) + "\t" + 
			           //Doubles.join("\t", policyOutput) + "\t" +
			           chooseTime + "\t" + udfTime + "\t" + labelTime + "\t" + trainingTime + "\t" + processingTime + "\t" + results[0]);

			// 5.  Exit if appropriate
			//if ((minPerformance >= 0.0 && performance >= minPerformance) ||
			//		(maxRuntime > 0 && runtimeSoFar >= maxRuntime)) {
			//	break;
			//}
			int numItemsToProcess = Integer.valueOf(ExperimentParameters.get("numItemsToProcess", "500000"));
			if (numBlocks == numItemsToProcess) { //numToProcess) {
				System.out.println("Run time: " + (float) runtimeSoFar/1000 + " s");
				
				Collections.sort(chooseTimes);
				// Not exactly, but close enough
				long medianChoose = chooseTimes.get(chooseTimes.size()/2);
				double meanChoose = (double) totalChooseTime/(numBlocks-1);
				System.out.println("Median choose time: " + (double) medianChoose/1000000 + " ms");
				System.out.println("Mean choose time: " + meanChoose/1000000 + " ms");
				System.out.println("Mean UDF time: " + ((double) totalUDFTime/(numBlocks))/1000000 + " ms");
				System.out.println("Mean labelling time: " + ((double) totalLabelTime/(numBlocks))/1000000 + " ms");
				System.out.println("Total choose time: " + ((double) totalChooseTime)/1000000000 + " s");
				System.out.println("Total UDF time: " + ((double) totalUDFTime)/1000000000 + " s");
				System.out.println("Total labelling time: " + ((double) totalLabelTime)/1000000000 + " s");
				System.out.println("Total training time: " + ((double) totalTrainingTime)/1000000000 + " s");
				
				PrintWriter pw2 = new PrintWriter(new FileOutputStream("test_data/output/" + testName + ".ids.txt"));
				for(Integer id : ltask.getItemIds()) {
					pw2.println(id);
				}
				pw2.close();
				break;
			}
		}

		//UDFFeatures.indexFeatureSet("image", ltask.getDescription(), udfList.get(0).getDescription(), ltask.getAllItems());

		pw.close();
	}
	
	public double getAoc() {
		return aoc;
	}
}
