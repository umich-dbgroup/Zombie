package edu.umich.eecs.featext;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {
	private String configFileName = "./config/config.properties";
	private Properties prop;
	
	public Config() { 
	   	this.load();
	}
	
	public Config(String fname) {
		configFileName = fname;
	   	this.load();
	}
	
	public String get(String propName) {
		return this.prop.getProperty(propName);
	}

	public void set(String propName, String value) {
		prop.setProperty(propName, value);
	}
	
	public void load() {
		this.prop = new Properties();
    	try {
            //load a properties file from class path, inside static method

    		prop.load(new FileInputStream(this.configFileName));
 
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }
	}
	
	public void save() {
	   	try {
    		//save properties to project root folder
    		this.prop.store(new FileOutputStream(this.configFileName), null);
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }
	}
	
	public static void main(String[] args) {
		Config conf = new Config();
		conf.set("testProperty", "TESTING123");
		conf.save();
		System.out.println(conf.get("testProperty"));
	}
}
