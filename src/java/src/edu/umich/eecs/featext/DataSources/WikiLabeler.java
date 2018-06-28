package edu.umich.eecs.featext.DataSources;

import java.io.DataOutputStream;
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

public class WikiLabeler implements Labeler {
	HashMap<Integer, String> imgLabels = new HashMap<Integer, String>();
	HashMap<String, ArrayList<Integer>> labelImgs = new HashMap<String, ArrayList<Integer>>();
	ArrayList<Integer> ids = new ArrayList<Integer>();

	HashMap<Integer, String> labelCache = new HashMap<Integer, String>();

	ItemCache itemCache;

	Random rand = new Random();

	 private static String[] classNames6 = {"geography", "sports", "videoGames", "science", "politics", "other"};
	 private static String[] classNames4 = {"geography", "sports", "videoGames", "other"};
	 private static String[] classNames2 = {"geography", "other"};

	 public static String[] classNames = classNames6;


	private Set<String> labelSet = new HashSet<String>();
	private double mislabelRate;
	private Set<Integer> mislabelIds;
	public WikiLabeler() {
     if (ExperimentParameters.check("numClasses", "2"))
        classNames = classNames2;
     else if (ExperimentParameters.check("numClasses", "6"))
        classNames = classNames6;

		for (String lbl : getLabels()) {
			labelSet.add(lbl);
		}

		itemCache = ContextualBanditPolicy.itemCache;
	}

	public String getLabel(int pageId) {
		if (labelCache.containsKey(pageId) && !(mislabelIds != null && mislabelIds.contains(pageId)) ) {
			return labelCache.get(pageId);
		}
		else {
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

			if (mislabelIds != null && !ExperimentParameters.check("noisyFeatures", "true") && mislabelIds.contains(pageId) && rand.nextDouble() < mislabelRate) {
				int newIdx = rand.nextInt(classNames.length - 1);
				//String oldLabel = classGuess;
				for (String lbl : classNames) {
					if (lbl.equals(classGuess)) continue;
					if (newIdx == 0) classGuess = lbl;
					newIdx--;
				}
				//String newLabel = classGuess;
				//System.out.println("Changed label: " + oldLabel + " to " + newLabel);
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

	}

	public void buildLabelCache(List<Integer> pageIds, List<Integer> labelIds) {
		int id = 1;
		Map<Integer, String> labels = new HashMap<Integer, String>();

		for (String lbl : classNames) {
			if (!lbl.equals("other")) {
				labels.put(id, lbl);
			}
			id++;
		}

		for (int i = 0; i < pageIds.size(); i++) {
			if (labels.get(labelIds.get(i)) != null)
				labelCache.put(pageIds.get(i), labels.get(labelIds.get(i)));
			else
				labelCache.put(pageIds.get(i), "other");
		}
	}

	// mislabelIds is *all possible* ids that can be mislabeled
	// the mislabelRate acts upon that set to pick the actual mislabeled ones
	public void setMislabelSet(Set<Integer> itemIds, double mislabelRate) {
		mislabelIds = new HashSet<Integer>();

		List<Integer> others = new ArrayList<Integer>();
		
		for (int id : itemIds) {
			String lbl = getLabel(id);
			if (!lbl.equals("other")) mislabelIds.add(id);
			else others.add(id);
		}
		
		Collections.shuffle(others);
		int nonOtherCount = mislabelIds.size();
		for (int i = 0; i < nonOtherCount; i++) {
			mislabelIds.add(others.get(i));
		}
		
		System.out.println("Actual mislabels: " + mislabelIds.size());
		this.mislabelRate = mislabelRate;
	}

	//	public static void main(String[] args) throws IOException {
	//		WikipediaDataSource data = new WikipediaDataSource();
	//		List<Integer> pageIds = data.loadDocIds();
	//		Labeler labeler = new WikiLabeler();
	//
	//		String outFile = "test_data/wikipedia/labels.bin";
	//
	//		Map<String, Integer> labelIds = new HashMap<String, Integer>();
	//
	//		int i = 1;
	//		for (String label : labeler.getLabels()) {
	//			if (label.equals("other")) labelIds.put(label, -1);
	//			else labelIds.put(label, i);
	//			i++;
	//		}
	//
	//		int n = 0;
	//		DataOutputStream os = new DataOutputStream(new FileOutputStream(outFile));
	//		for (int pageId : pageIds) {
	//			int id = labelIds.get(labeler.getLabel(pageId));
	//			os.writeShort(id);
	//			n++;
	//			if (n % 10000 == 0) System.out.println(n);
	//		}
	//		os.close();
	//	}

}
