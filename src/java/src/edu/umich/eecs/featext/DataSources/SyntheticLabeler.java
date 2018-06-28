package edu.umich.eecs.featext.DataSources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.Policies.ItemCache;

public class SyntheticLabeler implements Labeler {
	HashMap<Integer, String> imgLabels = new HashMap<Integer, String>();
	HashMap<String, ArrayList<Integer>> labelImgs = new HashMap<String, ArrayList<Integer>>();
	ArrayList<Integer> ids = new ArrayList<Integer>();
	
	SyntheticDataSource data;
	
	HashMap<Integer, String> labelCache = new HashMap<Integer, String>();
	
	ItemCache itemCache;
	
//	public static final String[] classNames = {"geography", "science", "sports", "politics", "videoGames", "other"};
//	public static final String [] classNames = {"geography", "sports", "videoGames", "other"};
	private String[] classNames;
	
	private Set<String> labelSet = new HashSet<String>();

	public SyntheticLabeler(SyntheticDataSource data, int classCount) {
		classNames = new String[classCount];
		for (int i = 0; i < classCount -1; i++) {
			classNames[i] = "c" + i;
		}
		classNames[classCount - 1] = "other";

		
		for (String lbl : getLabels()) {
			labelSet.add(lbl);
		}
		
		
		this.data = data;
		
		itemCache = data.getItemCache();
	}

	public String getLabel(int pageId) {
		if (labelCache.containsKey(pageId)) {
			return labelCache.get(pageId);
		}
		else {
			String text = null;
			Item item = data.getItem(pageId);
			
			if (item.getContents() != null) {
				text = item.getContents().toString();
			}
			else {
				System.out.println("No item loaded!");
			}
	
			String toks[] = text.split(" ");
			
			String classGuess = "other";
			for (String lbl : classNames) {
				if (lbl.equals(toks[0])) {
					classGuess = lbl;
					break;
				}
			}
			
			labelCache.put(pageId, classGuess);
			
			return classGuess;	
		}
	}

	public String[] getLabels() {
		return classNames;
	}

	@Override
	public void setDataSource(DataSource data) {
		// TODO Auto-generated method stub
		
	}

}
