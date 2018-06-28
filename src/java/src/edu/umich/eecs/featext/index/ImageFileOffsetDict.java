package edu.umich.eecs.featext.index;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;


public class ImageFileOffsetDict extends EntityIndex {
	private String dbName = "imageOffsetDict";
	public ImageFileOffsetDict() {
		this.init(this.dbName);
	}
	
	// this is for the docId -> file offset dictionary
	public void put(int key, FileOffset value) {
		try {
            DatabaseEntry dbKey = new DatabaseEntry();
            DatabaseEntry dbValue = new DatabaseEntry(value.toBytes()); 

            // Convert the Integer to bytes
            IntegerBinding.intToEntry(key, dbKey);

            this.db.put(null, dbKey, dbValue);
        } catch (Exception e) {
            System.out.println("Database error");
            e.printStackTrace();
        }	
	}
	
	public FileOffset get(int key) {
		FileOffset returnVal = null;
		
		try {
			DatabaseEntry dbKey = new DatabaseEntry();
			DatabaseEntry dbValue = new DatabaseEntry();

            // Convert the Integer to bytes
            IntegerBinding.intToEntry(key, dbKey);
			
			if (this.db.get(null, dbKey, dbValue, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				returnVal = new FileOffset(dbValue.getData());
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
