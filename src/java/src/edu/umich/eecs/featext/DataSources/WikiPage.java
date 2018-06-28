package edu.umich.eecs.featext.DataSources;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import edu.umich.eecs.featext.index.DocumentFeaturesIndex;
import edu.umich.eecs.featext.index.EntityIndex;
import edu.umich.eecs.featext.index.FeatureNameDict;
import edu.umich.eecs.featext.index.FileOffset;
import edu.umich.eecs.featext.index.IndexFactory;
import edu.umich.eecs.featext.index.ItemRetrievalException;
import edu.umich.eecs.featext.index.PostingsList;
import edu.umich.eecs.featext.index.WikiFileOffsetDict;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


public class WikiPage {
	int pageId;
	String title;
	String text;
	static Configuration conf = new Configuration();
	//static DocumentFeaturesIndex docIndex = new DocumentFeaturesIndex();
	public static WikiFileOffsetDict offsetDict = new WikiFileOffsetDict();

	public WikiPage() {}

	public WikiPage(int pageId, String title, String text) {                
		this.pageId = pageId;
		this.title = title;
		this.text = text; // Need to handle UTF-8?)
	}

	// TODO: this is horrible
	public WikiPage(int pageId) throws IOException, InstantiationException, IllegalAccessException  {
		ArrayList<Integer> p = new ArrayList<Integer>();
		p.add(pageId);
		List<WikiPage> lp = WikiPage.loadFromFile(p);
		this.pageId = lp.get(0).pageId;
		this.title = lp.get(0).title;
		this.text = lp.get(0).text;
	}

	/**
	 * Returns a list wiki pages from the large single file of 
	 * pages, based on the list of pageIds passed in.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static List<WikiPage> loadFromFile(List<Integer> pageIds) throws IOException, InstantiationException, IllegalAccessException {
		List<WikiPage> wpList = new ArrayList<WikiPage>();

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("test_data/wikipedia/wiki.seq");

		SequenceFile.Reader in = new SequenceFile.Reader(fs, p, conf);
		IntWritable key = (IntWritable) in.getKeyClass().newInstance();
		Text value = (Text) in.getValueClass().newInstance();

		for (int pageId : pageIds) {
			// Get the offset and length, stored in this object
			FileOffset fo = WikiPage.offsetDict.get(pageId);

			in.seek(fo.getOffset());
			in.next(key, value);
			String[] lines = value.toString().split(System.getProperty("line.separator"));
			String title = lines[0];
			String[] bodyLines = Arrays.copyOfRange(lines, 1, lines.length);
			WikiPage wp = new WikiPage(pageId, title, StringUtils.join(bodyLines, System.getProperty("line.separator")));
			wpList.add(wp);

		}

		in.close();

		return wpList;
	}

	/**
	 * Returns HashMap of tokens present in the Wiki page. Keys are the token
	 * strings, values are numbers of occurrences.
	 */
	public HashMap<String, Integer> getAllTokens() {
		String NOSPACE = String.valueOf((char)0xA0);
		String text = this.text;
		HashMap<String, Integer> tokens = new HashMap<String, Integer>();

		Map<String, Boolean> stoplist = this.getStopList();

		// Replace URLs with a general token and the domain

		// Replaced the one below with the simpler one. It was having a stack 
		// overflow on pageId = 23753295 (Super-long URL in it);
		//String pattern = "((https?):((//)|(\\\\\\\\))+([\\w\\d.-]*)([\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&](#!)?)+)";
		String pattern = "((https?):((//)|(\\\\\\\\))+([\\w\\d.-]*)([^\\s\\]\\|]+))";

		text = text.replaceAll(pattern, " EXT_LINK $6 ").replace('.', '|');

		// Get rid of some messy <ref> tags
		pattern = "(<ref>\\[?|\\]?</ref>)";
		text = text.replaceAll(pattern, "");


		// TODO: this section doesn't parse out tags properly.

		/*		// Make wiki links unsplitable. we'll add in a special char for spaces
		// and get rid of the alt-text for the link, as well
		pattern = "(\\[\\[(([^#\\]|]|\\]+(?=[^\\]])))(\\|(([^\\]]|\\](?=[^\\]]))*))?\\]\\])";

		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(text);
		while (m.find()) {
			String tag = m.group(1);
			String name = StringUtils.replace(m.group(2)," ", NOSPACE);
			String altText = StringUtils.replace(m.group(4)," ", NOSPACE);
			if (altText == null) altText = "";
			text = StringUtils.replace(text, tag, " ||||" + name + " " + altText);
		}*/

		String[] words = StringUtils.split(text);

		for (int i = 0; i < words.length; i++) {
			words[i] = StringUtils.replace(words[i], NOSPACE, " ");
			words[i] = StringUtils.strip(words[i]);
			words[i] = StringUtils.lowerCase(words[i]);

			// Remove leading and trailing punctuation
			//words[i] = words[i].replaceAll("[^\\w]+$","");
			//words[i] = words[i].replaceAll("^[^\\w\\|{]+","");

			// This is much faster than the above leading and trailing punctuation
			// removal. Here we just get rid of all the non-word chars + some.
			words[i] = words[i].replaceAll("[^\\w{| ]","");

			if (words[i] != null && words[i].length() > 0 && !stoplist.containsKey(words[i])) {
				Integer count = tokens.get(words[i]);
				if (count == null) {
					tokens.put(words[i], 1);
				} else {
					tokens.put(words[i],  count+1);
				}
				//System.out.println(words[i]);
			}
		}
		return tokens;
	}

	public int getPageId() {
		return this.pageId;
	}

	public String getTitle() {
		return this.title;
	}

	public String getText() {
		return this.text;
	}


	/**
	 * Returns Map of stop list words. List found somewhere on Internet and 
	 * not specifically tailored to Wikipedia, except for a few additions.
	 */
	public Map<String, Boolean> getStopList() {
		String list = "a,able,about,across,after,all,almost,also,am,among,an,";
		list += "and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,";
		list += "did,do,does,either,else,ever,every,for,from,get,got,had,has,";
		list += "have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,";
		list += "just,least,let,like,likely,may,me,might,most,must,my,neither,";
		list += "no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,";
		list += "say,says,she,should,since,so,some,than,that,the,their,them,then,";
		list += "there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,";
		list += "when,where,which,while,who,whom,why,will,with,would,yet,you,your,";
		list += "redirect,refer";

		String[] words = StringUtils.split(list, ',');
		Map<String, Boolean> m = new HashMap<String, Boolean>();
		for (String key : words) {
			m.put(key, true);
		}
		return m;
	}

	/**
	 * Returns HashMap of features present in the WikiPage. Key is featureId,
	 * value is a Float "score", which may just be number of token occurrences
	 * in the page. The scores are not normalized in any way, so that may need
	 * to be done prior to use.
	 * @throws ItemRetrievalException 
	 */
	public HashMap<Integer, Float> getFeatures() throws ItemRetrievalException {  	
		PostingsList item = IndexFactory.docFeatIdx.get(this.pageId);  	
		return item.getPostings();
	}

	public static int getLength(int pageId) throws IOException, InstantiationException, IllegalAccessException {
		WikiPage wp = new WikiPage(pageId);
		return wp.getText().length();
	}

	public static void main(String[] args) {
		// Open the file
		FileInputStream fstream;
		WikiLabeler labeler = new WikiLabeler();
		try {
			fstream = new FileInputStream("test_data/output/01-15_2class_context_10000.1_ids.txt");

			BufferedReader br = new BufferedReader(new InputStreamReader(fstream, "UTF8"));

			String strLine;

			HashMap<Integer, Integer> docLengths = new HashMap<Integer, Integer>();
			HashMap<Integer, Integer> docLabels = new HashMap<Integer, Integer>();

			//Read File Line By Line
			while ((strLine = br.readLine()) != null)   {
				// Print the content on the console
				int id = Integer.valueOf(strLine.trim());
				int length = WikiPage.getLength(id);
				docLengths.put(id, length);
				String label = labeler.getLabel(id);
				
				int labelId = 1;
				if (label.equals("other")) labelId = 0;
				
				docLabels.put(id, labelId);
			}

			//Close the input stream
			br.close();
			
		    FileWriter foutstream = new FileWriter("test_data/docLengths_10000.txt");
		    BufferedWriter out = new BufferedWriter(foutstream);

		    for (Map.Entry<Integer, Integer> entry : docLengths.entrySet()) {		        
		        out.write(entry.getKey() + "\t" + entry.getValue() + "\t" + docLabels.get(entry.getKey()) + "\n");
		    }

		    out.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		/*
		System.out.println("Hello World!!!");

		//FeatureNameDict fn = IndexFactory.featureNamesIdx;

		List<Integer> pageIds = new ArrayList<Integer>();
		//pageIds.add(2154);
		pageIds.add(21458604);
		//pageIds.add(15079013); // last page id in file
		// pageIds.add(36852080);

		//pageIds.add(12);

		List<WikiPage> pages = null;
		try {
			pages = WikiPage.loadFromFile(pageIds);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (WikiPage wp : pages) {
			System.out.println(wp.getTitle() + "\n" + wp.getText() +"\n----\n");
			//wp.getAllTokens();
//			HashMap<Integer, Float> hm;
//			try {
//				hm = wp.getFeatures();
//
//				for (Map.Entry<Integer, Float> entry : hm.entrySet()) {
//					System.out.println(fn.get(entry.getKey()) + " = " + entry.getValue());
//				}
//
//			} catch (ItemRetrievalException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		 */
	}
}
