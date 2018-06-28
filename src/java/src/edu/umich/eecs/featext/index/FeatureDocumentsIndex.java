package edu.umich.eecs.featext.index;

import java.util.Map;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;


public class FeatureDocumentsIndex extends EntityIndex {
	private String dbName = "invertedIdx";
	
	public FeatureDocumentsIndex() {
		this.init(dbName);
	}
	
	public FeatureDocumentsIndex(String dbName) {
		this.dbName = dbName;
		this.init(this.dbName);
	}
	
	// this is for the docId -> file offset dictionary
	public void put(PostingsList item) {
		try {
            DatabaseEntry dbKey = new DatabaseEntry();
            DatabaseEntry dbValue = item.objectToEntry(); 

            IntegerBinding.intToEntry(item.getKey(), dbKey);
            
            this.db.put(null, dbKey, dbValue);
        } catch (Exception e) {
            System.out.println("Database error");
            e.printStackTrace();
        }	
	}
	
	public void merge(PostingsList item) throws ItemRetrievalException {
		PostingsList existing = null;
		if ((existing = this.get(item.getKey())) == null) {
			existing = item;
		}
		else {
			for (Map.Entry<Integer, Float> entry : item.getPostings().entrySet()) {
				existing.addItem(entry.getKey(), entry.getValue());
			}
		}
		this.put(existing);
	}
	
	public PostingsList get(int key) throws ItemRetrievalException {
		PostingsList item = new PostingsList(key);
		try {
			DatabaseEntry dbKey = new DatabaseEntry();
			DatabaseEntry dbData = new DatabaseEntry();	    
			
            IntegerBinding.intToEntry(item.getKey(), dbKey);
			
            // Convert the Integer to bytes
           // this.getIntBinding().objectToEntry(item.getKey(), dbKey);
			
			if (this.db.get(null, dbKey, dbData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				
				item = new PostingsList(dbData);
			}
			else {
				//System.out.println("Item not found: " + key);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new ItemRetrievalException();
		}
		return item;
	}
}
