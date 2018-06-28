package edu.umich.eecs.featext.index;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.DatabaseEntry;

public class PostingsList {
	int key;
	HashMap<Integer,Float> postings;
	
	public PostingsList(int key) {
		this.key = key;
		this.postings = new HashMap<Integer,Float>();
	}
	
	public PostingsList(int key, HashMap<Integer,Float> postings) {
		this.key = key;
		this.postings = postings;
	}
	
	
	/**
	 * Initializes a PostingsList from a BDB entry. This takse the place
	 * of the normal TupleBinding.entryToObject() method.
	 */
	public PostingsList(DatabaseEntry entry) {
		TupleInput input = TupleBinding.entryToInput(entry);
		
		// Figure out how many records. First 4 bytes are the int list key.
		// The rest are 8 byte posting pairs, int key, float value.
		int byteCount = entry.getSize();
		int recordCount = 0;
		if (byteCount > 0) recordCount = (byteCount - 4)/8;

		// Read the data. List key first, then loop over the rest.
		this.key = input.readInt();
		this.postings = new HashMap<Integer,Float>();
		for (int i = 0; i < recordCount; i++) {
			this.postings.put(input.readInt(), input.readFloat());
		}
	}
	

	public DatabaseEntry objectToEntry() {
		TupleOutput output = new TupleOutput();
		DatabaseEntry entry = new DatabaseEntry();
		
		output.writeInt(this.key);
		
    	for (Map.Entry<Integer, Float> posting : this.postings.entrySet()) {
    		output.writeInt(posting.getKey());
    		output.writeFloat(posting.getValue());
    	}
		
    	TupleBinding.outputToEntry(output, entry);
    	
		return entry;
	}
	

	public void addItem(int itemKey, Float value) {
		this.postings.put(itemKey, value);
	}
	
	public void incrementItem(int itemKey, Float value) {
		Float oldVal = postings.get(itemKey);
		if (oldVal == null) {
			addItem(itemKey, value);
		}
		else {
			addItem(itemKey, value + oldVal);
		}
	}
	
	public int getKey() {
		return this.key;
	}
	
	public HashMap<Integer,Float> getPostings() {
		return postings;
	}
	
	public static void main(String[] args) {

	}
}

