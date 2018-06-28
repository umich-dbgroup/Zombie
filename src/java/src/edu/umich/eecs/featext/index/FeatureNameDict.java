package edu.umich.eecs.featext.index;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class FeatureNameDict extends EntityIndex {
	private String dbName = "featureNameDict";
	public FeatureNameDict() {
		init(dbName);
	}
	public FeatureNameDict(String dbName) {
		this.dbName = dbName;
		init(dbName);
	}
	
	// this is for the termId -> term dictionary
	public void put(Integer key, String value) {
		try {
            DatabaseEntry dbKey = new DatabaseEntry(); 
            DatabaseEntry dbValue = new DatabaseEntry();

            IntegerBinding.intToEntry(key, dbKey);
            StringBinding.stringToEntry(value, dbValue);
            
            this.db.put(null, dbKey, dbValue);
        } catch (Exception e) {
            System.out.println("Database error");
            e.printStackTrace();
        }
	}
	
	public String get(int key) {
		String returnVal = null;
		
		try {
			DatabaseEntry dbKey = new DatabaseEntry();
			DatabaseEntry dbValue = new DatabaseEntry();
		    
            IntegerBinding.intToEntry(key, dbKey);
			
			if (this.db.get(null, dbKey, dbValue, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	            returnVal = StringBinding.entryToString(dbValue);

			}
			else {
				//System.out.println("Item not found: " + key);
			}
		}
		catch (Exception e) {
			System.out.println("Problem getting item: " + key);
			e.printStackTrace();
		}
		return returnVal;
	}
}
