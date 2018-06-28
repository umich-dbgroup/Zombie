package edu.umich.eecs.featext;

import java.util.HashMap;
import java.util.List;

interface UDF {
	List<String> execute(HashMap<String, String> input); 
	boolean test(HashMap<String, String> input); 
	String getName();
}
