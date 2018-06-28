package edu.umich.eecs.featext.harness;

import java.util.HashMap;

public class ExperimentParameters {
	static HashMap<String, String> params = new HashMap<String, String>();
	
	public static void set(String paramName, String value) {
		params.put(paramName, value);
	}
	
	public static String get(String paramName) {
		return params.get(paramName);
	}
	
	public static String get(String paramName, String defaultValue) {
		String val = params.get(paramName);
		if (val == null) return defaultValue;
		else return val;
	}
	
	public static double get(String paramName, double defaultValue) {
		String val = params.get(paramName);
		if (val == null) return defaultValue;
		else return Double.valueOf(val);
	}
	
	public static int get(String paramName, int defaultValue) {
		String val = params.get(paramName);
		if (val == null) return defaultValue;
		else return Integer.valueOf(val);
	}
	
	public static boolean check(String param, String value) {
		if (params.get(param) != null && params.get(param).equals(value)) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public static void reset() {
		params.clear();
	}
}
