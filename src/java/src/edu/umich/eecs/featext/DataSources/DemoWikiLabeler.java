package edu.umich.eecs.featext.DataSources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;

import edu.umich.eecs.featext.Policies.ContextualBanditPolicy;
import edu.umich.eecs.featext.Policies.Item;
import edu.umich.eecs.featext.Policies.ItemCache;

public class DemoWikiLabeler implements Labeler {
	HashMap<Integer, String> imgLabels = new HashMap<Integer, String>();
	HashMap<String, ArrayList<Integer>> labelImgs = new HashMap<String, ArrayList<Integer>>();
	ArrayList<Integer> ids = new ArrayList<Integer>();

	ItemCache itemCache;

	public static final String[] classNames = {"geography", "science", "sports", "politics", "videoGames", "other"};
	//	public static final String [] classNames = {"geography", "sports", "videoGames", "other"};
	//public static final String [] classNames = {"geography", "other"};

	private Set<String> labelSet = new HashSet<String>();
	public DemoWikiLabeler() {
		for (String lbl : getLabels()) {
			labelSet.add(lbl);
		}

		itemCache = ContextualBanditPolicy.itemCache;
	}

	public String getLabel(String text) {
		String pat = "\\[Category:([^\\r\\n\\[\\|\\]]*)[\\]\\|]";
		Pattern p = Pattern.compile(pat);
		Matcher m = p.matcher(text);

		String[] science = {"biology", "chemistry", "physics", 
				"meteorology","astronomy", "geology", "genetic"};

		String[] sports = {"baseball", "football", "basketball", "hockey", "soccer",};
		//"tennis", "golf", "curling", "skiing", "olympics", "sports", 
		//"swimming", "athletic", "athlete"};

		String[] basketball = {"basketball"};

		String[] politics = {"president", "senate", "congress", "election", "mayor", 
				"governor", "voting", "council", "king", "queen", "parliament"};

		String[] videoGames = {"video game", "arcade game", "windows game", "dos game"};

		String[] geography = {"cities in", "counties", "states of", "countries in"};

		String classGuess = "other";


		String categoryText = "";

		while (m.find()) {
			String category = m.group(1).toLowerCase();
			categoryText += " " + category;
		}

		if (labelSet.contains("science")) {
			Pattern sciPattern = Pattern.compile(Joiner.on("|").join(science));

			Matcher matcher = sciPattern.matcher(categoryText);
			if (matcher.find()) {
				classGuess = "science";
			}
		}

		if (labelSet.contains("geography")) {
			Pattern geoPattern = Pattern.compile(Joiner.on("|").join(geography));

			Matcher matcher = geoPattern.matcher(categoryText);
			if (matcher.find()) {
				classGuess = "geography";
			}
		}

		if (labelSet.contains("basketball")) {
			Pattern basketballPattern = Pattern.compile(Joiner.on("|").join(basketball));

			Matcher matcher = basketballPattern.matcher(categoryText);
			if (matcher.find()) {
				classGuess = "sports";
			}
		}

		if (labelSet.contains("sports")) {
			Pattern sportsPattern = Pattern.compile(Joiner.on("|").join(sports));

			Matcher matcher = sportsPattern.matcher(categoryText);
			if (matcher.find()) {
				classGuess = "sports";
			}
		}

		if (labelSet.contains("videoGames")) {
			Pattern vgamesPattern = Pattern.compile(Joiner.on("|").join(videoGames));

			Matcher matcher = vgamesPattern.matcher(categoryText);
			if (matcher.find()) {
				classGuess = "videoGames";
			}
		}

		if (labelSet.contains("politics")) {
			Pattern poliPattern = Pattern.compile(Joiner.on("|").join(politics));

			Matcher matcher = poliPattern.matcher(categoryText);
			if (matcher.find()) {
				classGuess = "politics";
			}
		}

		return classGuess;	
	}

	public String[] getLabels() {
		return classNames;
	}

	@Override
	public void setDataSource(DataSource data) {

	}

	@Override
	public String getLabel(int docId) {
		// TODO Auto-generated method stub
		return null;
	}

}
