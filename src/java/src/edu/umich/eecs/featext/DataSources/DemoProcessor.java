package edu.umich.eecs.featext.DataSources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Policies.SerialContextualBanditPolicy;
import edu.umich.eecs.featext.Tasks.CollectionTask;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.UDFs.LabelerUDF;
import edu.umich.eecs.featext.UDFs.UDF;
import edu.umich.eecs.featext.harness.ExperimentParameters;

public class DemoProcessor	{
	private Path fname;
	private List<Item> itemList;
	private DataSource data;
	private LearningTask ltask;
	private Policy policy;
	ArrayList<UDF> udfList;
	
	private String testName;

	
	private Map<String, Double> statusMap = new HashMap<String, Double>();

	public DemoProcessor(String fileName) {
		//ExperimentParameters.set("armBudget", "10000");
		//ExperimentParameters.set("pruneType", "bySize");
		
		fname = new Path(fileName);
		itemList = new ArrayList<Item>();
		data = new DemoDataSource(fileName);
		
		udfList = new ArrayList<UDF>();
		
		udfList.add(LabelerUDF.createUDF("keywords", data.getLabeler()));

		policy = SerialContextualBanditPolicy.createPolicy("demo");
        String taskDesc = "collectionTask";
        ltask = new CollectionTask(taskDesc, data, udfList, "sports");
		
		testName = "demo_" + fileName;
		
		statusMap.put("total", 0.0);
		statusMap.put("matches", 0.0);
		
		loadData();
	}

	private void loadData() {
		policy.init(data, ltask, fname);
	}
	
	public List<Item> getItemList() {
		return itemList;
	}

	public void run() {		
		System.out.println("**** RUN!");
		ltask.setFilePrefix("test_data/output/" + testName + ".");
		double aoc = 0.0;
		int itemsProcessed = 0;
		while (true) {
			// 1.  Fetch the next data block
			DataBlock block = policy.getNextDataBlock();
			
			if (block == null) {
				break;
			}
			
			// 2.  Process the data block
			for(UDF udf : udfList) {
				udf.processBlock(block, ltask);
			}
		
			itemsProcessed++;
			
			// 3.  Update statistics
			//double performance = ltask.getPerformance();
			double[] results = ltask.getResults();

			aoc += results[0];
			//System.out.println(results[0]);
			statusMap.put("matches", results[0]);
			statusMap.put("total", (double) itemsProcessed);
		}
		statusMap.put("aoc", (double) aoc/(itemsProcessed * statusMap.get("matches")));
		
		
	}
	
	public Map<String, Double> getStatusInfo() {
		return statusMap;
	}
	
	public static void main(String[] args) {
		double aoc = 0;
		int runs = 5;
		for (int i = 0; i < runs; i++) {		
			DemoProcessor dp = new DemoProcessor("test_data/wikipedia/000.seq");
	
			long time1 = System.nanoTime();
			dp.run();
			long time2 = System.nanoTime();
			System.out.println("Time: " + (double)(time2 - time1)/1000000 + " ms");
			
			System.out.println(dp.getStatusInfo());
			aoc += dp.getStatusInfo().get("aoc");
		}
		System.out.println("--------------");
		System.out.println("Avg. AOC: " + aoc/runs);
	}
}


