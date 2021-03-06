package edu.umich.eecs.featext.harness;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.fs.Path;

import edu.umich.eecs.featext.DataSources.DataSource;
import edu.umich.eecs.featext.DataSources.WikipediaDataSource;
import edu.umich.eecs.featext.Policies.Policy;
import edu.umich.eecs.featext.Policies.SerialContextualBanditPolicy;
import edu.umich.eecs.featext.Tasks.BasicClassifierTask;
import edu.umich.eecs.featext.Tasks.LearningTask;
import edu.umich.eecs.featext.Tasks.LogRegClassifierTask;
import edu.umich.eecs.featext.UDFs.KeyWordsUDF;
import edu.umich.eecs.featext.UDFs.UDF;


public class VaryTrainingInterval {
	public static void main(String[] args) {
        String[] newArgs = new String[args.length+2];
        String[] intervals = {"10","50","100","500","1000"};

        for (int i = 0; i < args.length; i++) {
            newArgs[i] = args[i];
        }

        newArgs[args.length] = "0";
        
        for (String lvl : intervals) {
            newArgs[args.length + 1] = lvl;
            TestSuite.main(newArgs);
        }
	}
}
