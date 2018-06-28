package edu.umich.eecs.featext.index;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;


public class FeatureDict extends EntityIndex {
	private String dbName = "featureDict";
	public FeatureDict() {
		init(dbName);
	}
	public FeatureDict(String dbName) {
		this.dbName = dbName;
		init(dbName);
	}
	
	public int get(String key) throws ItemRetrievalException {
		int returnVal = -1;
		
		try {
			DatabaseEntry theKey = new DatabaseEntry();
			DatabaseEntry dbValue = new DatabaseEntry();
		    
			StringBinding.stringToEntry(key, theKey);
			
			if (this.db.get(null, theKey, dbValue, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	            returnVal = IntegerBinding.entryToInt(dbValue);
			}
			else {
				//System.out.println("Item not found: " + key);
			}
		}
		catch (Exception e) {
			throw new ItemRetrievalException();
		}
		return returnVal;
	}
}
