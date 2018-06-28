package edu.umich.eecs.featext.index;

import com.sleepycat.je.*;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;

//import edu.umich.eecs.featext.Config;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;


public class EntityIndex {
	public static Configuration conf = new Configuration();
		
	private String dbName;
	protected Environment environment;
	private EnvironmentConfig envConfig;
	private DatabaseConfig dbConfig;
	protected Database db;
	public EntityIndex() {}
	private Cursor curs;
	
	public EntityIndex(String name) {
		this.init(name);
	}
		
	public void init(String name) {
		this.dbName = name;
		this.envConfig = new EnvironmentConfig();
		this.dbConfig = new DatabaseConfig();

		// All dbs will share a cache (recommended in docs).
		this.envConfig.setSharedCache(true);
        this.envConfig.setReadOnly(true);

		
		// Inserts not recorded until sync() or cache overflows
        this.dbConfig.setReadOnly(true);
		this.dbConfig.setDeferredWrite(true);
		
		try {
			this.environment = new Environment(this.getDbDir(), this.envConfig);
			this.db = this.environment.openDatabase(null, this.dbName, this.dbConfig);
		} catch (EnvironmentNotFoundException e) {
			System.out.println("Database Environment not found. Creating DB.");
			this.createDatabase();
		}
		catch (DatabaseNotFoundException e) {
			System.out.println("Database not found. Creating DB. " + e);
			this.createDatabase();
			this.db = this.environment.openDatabase(null, this.dbName, this.dbConfig);
		}
	}

	private File getDbDir() {
		File theDir = new File(EntityIndex.conf.get("bdbDir"), this.dbName);
		// if the directory does not exist, create it
		if (!theDir.exists()) {
			theDir.mkdir();  
		}
		return theDir;
	}
	
	public void createDatabase() {
		this.envConfig.setAllowCreate(true);
		this.dbConfig.setAllowCreate(true);

		this.environment = new Environment(this.getDbDir(), this.envConfig);
		this.db = this.environment.openDatabase(null, this.dbName, this.dbConfig);
		this.dbConfig.setAllowCreate(false);
		this.envConfig.setAllowCreate(false);
		this.environment = new Environment(this.getDbDir(), this.envConfig);
	}

	public void syncDatabase() {
		this.environment.sync();
	}
	
	public void startTraversal() {
		this.curs = this.db.openCursor(null, null);
	}
	
	public PostingsList getNext() {
		PostingsList item = null;
		
		// Cursors need a pair of DatabaseEntry objects to operate. These hold
	    // the key and data found at any given position in the database.
	    DatabaseEntry foundKey = new DatabaseEntry();
	    DatabaseEntry foundData = new DatabaseEntry();
		
		if (this.curs.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			item = new PostingsList(foundData);
		}
		
		return item;
	}
	
	public Integer getNextKey() {
		Integer nextKey = null;
		// Cursors need a pair of DatabaseEntry objects to operate. These hold
    // the key and data found at any given position in the database.
    DatabaseEntry foundKey = new DatabaseEntry();
    DatabaseEntry foundData = new DatabaseEntry();

		if (this.curs.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			nextKey = IntegerBinding.entryToInt(foundKey);
		}
		
		return nextKey;
	}
	
	public void endTraversal() {
		this.curs.close();
	}
	
	public int getMaxKey() {
		startTraversal();
		Integer nextKey = null;
		int maxId = 0;
		while ((nextKey = getNextKey()) != null) {
			if (nextKey > maxId) {
				maxId = nextKey; 
			}
		}
		endTraversal();
		return maxId;
	}
	
	public void close() {
        if (this.curs != null) {
        	this.curs.close();
        }

        if (this.db != null) {
        	this.db.close();
        }

        if (this.environment != null) {
        	this.environment.close();
        }
	}
	
	// this is for the term -> id dictionary
	public void put(String key, Integer value) {
		try {
            DatabaseEntry dbKey = new DatabaseEntry();
            DatabaseEntry dbValue = new DatabaseEntry(); 

            // Convert the Integer to bytes
            StringBinding.stringToEntry(key, dbKey);
            IntegerBinding.intToEntry(value, dbValue);

            this.db.put(null, dbKey, dbValue);
        } catch (Exception e) {
            System.out.println("Database error");
            e.printStackTrace();
        }	
	}

    public static void main(String[] args) {

    }
}    
    
