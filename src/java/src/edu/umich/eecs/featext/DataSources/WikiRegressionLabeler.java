package edu.umich.eecs.featext.DataSources;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;

import edu.umich.eecs.featext.Policies.ContextualBanditPolicy;
import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.Policies.ItemCache;
import edu.umich.eecs.featext.harness.ExperimentParameters;

public class WikiRegressionLabeler implements Labeler {
	HashMap<Integer, Double> pageValues = new HashMap<Integer, Double>();
	ArrayList<Integer> ids = new ArrayList<Integer>();

	String binFile = "test_data/wikipedia/page_id_lengths.bin";

	Random rand = new Random();
	
	ItemCache itemCache;
	private HashSet<Integer> mislabelIds;

	public WikiRegressionLabeler() {
		loadValues();

		itemCache = ContextualBanditPolicy.itemCache;
	}

	public String getLabelOld(int pageId) {
		return pageValues.get(pageId).toString();
	}

	public String getLabel(int pageId) {
		//		if (labelCache.containsKey(pageId) && !(mislabelIds != null && mislabelIds.contains(pageId)) ) {
		//			return labelCache.get(pageId);
		//		}
		//else {
		String text;
		Item item = itemCache.get(pageId);

		if (item.getContents() != null) {
			text = item.getContents().toString();
		}
		else {
			WikiPage wp = null;
			try {
				wp = new WikiPage(pageId);
			} catch (Exception e) {
				e.printStackTrace();
			} 
			text = wp.getText();
		}
		
		double val = (double)text.toString().length()/10100;
		
		if (mislabelIds != null && mislabelIds.contains(pageId)) {
			val = 10 * rand.nextGaussian() + 0.53;
		}
		
		return String.valueOf(val);
	}

	public String[] getLabels() {
		return new String[0];
	}

	@Override
	public void setDataSource(DataSource data) {

	}

	private void loadValues() {
		try {
			DataInputStream bin = new DataInputStream(new BufferedInputStream(new FileInputStream(binFile)));
			Integer id = null;
			Double value = null;
			int nonzero = 0;
			double sum = 0;
			while (true) {
				try {

					if (id == null) id = bin.readInt();
					else {
						value = (double) bin.readInt();
						//value = Math.log(value + 1)/1.6166; // divide by std dev.
						
						value = value/10100;


						id = null;
						value = null;
						
					}
				} catch(EOFException eof) {
					break;

				}
			}
			bin.close();
			System.out.println("Mean: " + sum/nonzero);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setMislabelSet(Set<Integer> itemIds, double mislabelRate) {
		mislabelIds = new HashSet<Integer>();

		mislabelIds.addAll(itemIds);
		
		System.out.println("Labeler mislabels: " + mislabelIds.size());
	}
	
}
