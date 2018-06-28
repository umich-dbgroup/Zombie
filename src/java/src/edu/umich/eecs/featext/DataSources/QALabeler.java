package edu.umich.eecs.featext.DataSources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.umich.eecs.featext.Policies.ItemCache;

public class QALabeler implements Labeler {
	HashMap<Integer, String> imgLabels = new HashMap<Integer, String>();
	HashMap<String, ArrayList<Integer>> labelImgs = new HashMap<String, ArrayList<Integer>>();
	ArrayList<Question> questions;
	HashMap<Integer, String> labelCache = new HashMap<Integer, String>();
	
	ItemCache itemCache;
	
	public static final String [] classNames = {"correct", "wrong"};
	
	private Set<String> labelSet = new HashSet<String>();
	public QALabeler() {
		for (String lbl : getLabels()) {
			labelSet.add(lbl);
		}
	}

	public void setQuestions(ArrayList<Question> questions) {
		this.questions = questions;
	}
	
	public String getLabel(int docId) {
		if (labelCache.containsKey(docId)) {
			return labelCache.get(docId);
		}
		else {
			if (questions.get(docId).getLabel().equals("wrong")) {
				return "wrong";
			}
			else {
				//System.out.println(docId + ": " + questions.get(docId).getQuestion() + "  " + questions.get(docId).getAnswer());
				return "correct";
			}
		}
	}

	public ArrayList<Integer> getImagesForLabel(String label) {
		return labelImgs.get(label);
	}

	public String[] getLabels() {
		return classNames;
	}

	@Override
	public void setDataSource(DataSource data) {
		// TODO Auto-generated method stub
		
	}
}
