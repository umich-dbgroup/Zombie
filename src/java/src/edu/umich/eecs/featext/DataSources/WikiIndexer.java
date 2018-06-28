package edu.umich.eecs.featext.DataSources;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import edu.umich.eecs.featext.Config;
import edu.umich.eecs.featext.index.DocumentFeaturesIndex;
import edu.umich.eecs.featext.index.FeatureDict;
import edu.umich.eecs.featext.index.FeatureDocumentsIndex;
import edu.umich.eecs.featext.index.FeatureNameDict;
import edu.umich.eecs.featext.index.FileOffset;
import edu.umich.eecs.featext.index.ImageFileOffsetDict;
import edu.umich.eecs.featext.index.ItemRetrievalException;
import edu.umich.eecs.featext.index.PostingsList;
import edu.umich.eecs.featext.index.WikiFileOffsetDict;


public class WikiIndexer {
	public static Config conf = new Config();
	public WikiIndexer() { }

	public void indexFileOffsets() throws IOException, InstantiationException, IllegalAccessException {
		HashSet<Integer> goodIds = new HashSet<Integer>(getPageIds(5000000, 0));

		//		WikiFileOffsetDict offsetDict = new WikiFileOffsetDict();
		ImageFileOffsetDict offsetDict = new ImageFileOffsetDict();

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		//  	Path p = new Path("test_data/wikipedia/wiki.seq");
		Path p = new Path("test_data/Caltech101.seq");

		SequenceFile.Reader in = new SequenceFile.Reader(fs, p, conf);
		IntWritable key = (IntWritable) in.getKeyClass().newInstance();

		long pos = 0;
		long nextPos = in.getPosition();

		int i = 0;

		// Calling next leaves the cursor at the position for the item after the
		// the one we are reading, so save that one for next time.
		while (in.next(key)) {
			pos = nextPos;
			nextPos = in.getPosition();

			int length = (int) (nextPos - pos);
			int pageId = key.get();

			//if (goodIds.contains(pageId)) {
			FileOffset fo = new FileOffset(pos, length);
			offsetDict.put(pageId, fo);

			if (i % 100 == 0) {
				System.out.println(i);
				offsetDict.syncDatabase();
			}
			i++;
			//}

		}
		offsetDict.syncDatabase();
		in.close();
	}

	public void indexWikiPages(int startAt) throws IOException, ItemRetrievalException, InstantiationException, IllegalAccessException {

		int n = startAt;
//
//		int chunkSize = 100;
//		ArrayList<Integer> ids = new ArrayList<Integer>();// (ArrayList<Integer>) getPageIds(chunkSize, startAt + chunkSize*i);
//		for (int i = 1; i <= 1503; i++) {
//			ids.add(i);
//		}
		int i = 0;
		DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex("wikiW2VDocIdx");
		//FeatureDocumentsIndex docIdx = new FeatureDocumentsIndex("wiki500InvIdx");

	//	FeatureDict featureDict = new FeatureDict("wiki500FeatureDict");	
	//	FeatureNameDict featureIdToName = new FeatureNameDict("wiki500FeatureNameDict");

	//	int nextDictKey = 1;

		//while (ids.size() > 0) {
		//List<WikiPage>wpList = WikiPage.loadFromFile(ids);		

		//for (WikiPage wp : wpList) {    
		String csvIn = "/z/wikipedia/w2v_features.txt";
		BufferedReader in = new BufferedReader(new FileReader(csvIn));

		String curLine = null;
		int pageId = 1;
		while ((curLine = in.readLine()) != null) {
			curLine = curLine.trim();
			String[] data = curLine.split("\t");

			pageId = Integer.parseInt(data[0]); // + 1;
			
			String[] feats = data[1].split(",");
			
			if (n % 1000 == 0) {
				System.out.println(n);// + ": " + wp.getPageId() + " " + wp.getTitle() + " " + nextDictKey);
			}

			if (n % 10000 == 0) {
				System.out.println("Syncing databases");
				docIdx.syncDatabase();
//				featureDict.syncDatabase();
//				featureIdToName.syncDatabase();	
				System.out.println("Done syncing databases");
			}
			//HashMap<String, Integer> tokens = wp.getAllTokens();
			PostingsList doc = new PostingsList(pageId);//wp.getPageId());

			// Loop through the tokens from the wiki page, and create
			// the doc->terms index and term dictionary.
			//for (Map.Entry<String, Integer> entry : tokens.entrySet()) {		
			for (int j = 0; j < feats.length; j++) {
				//String feature = Integer.toString(j + 1); //entry.getKey().toString();
				//String[] curFeat = feats[j].split("\\|");

				int featId = j + 1; 
				float value = Float.parseFloat(feats[j]); //new Float(entry.getValue().toString());

				if (value > 0) {
					doc.addItem(featId, value);
				}
				
				//int featId = -1;
				// See if the feature term is already in the dictionary.
				// If not, add it before we create the posting.
//				if ((featId = featureDict.get(feature)) == -1) {
//					featId = nextDictKey++;
//					featureDict.put(feature, featId);
//					featureIdToName.put(featId, feature);
//				}


				// Add the posting to the doc item
			}
			docIdx.put(doc);
			n++;
		//	pageId++;
		}

		// For the next loop iteration
		//i++;
		//ids = (ArrayList<Integer>) getPageIds(chunkSize, startAt + chunkSize*i);
		//}

		// Do one final sync() to make sure everything is flushed to disk.
		// Not sure if this is actually necessary.
		docIdx.syncDatabase();
	//	featureDict.syncDatabase();
	//	featureIdToName.syncDatabase();
	}

	/**
	 * importFeatureNames()
	 * 		loads feature names into bdb from a file
	 * @throws IOException
	 */
	public void importFeatureNames(String fileName, String dictName) throws IOException  {
		int n = 0;
	  FeatureNameDict featureIdToName = new FeatureNameDict(dictName);
		BufferedReader in = new BufferedReader(new FileReader(fileName));

		int nextDictKey = 1;

		String name = null;
		while ((name = in.readLine()) != null) {
			name = name.trim();

			if (n % 1000 == 0) {
				System.out.println(n);
			}

			if (n % 10000 == 0) {
				System.out.println("Syncing databases");
				featureIdToName.syncDatabase();
				System.out.println("Done syncing databases");
			}

			featureIdToName.put(nextDictKey, name);
			nextDictKey++;
			n++;
		}

		// Do one final sync() to make sure everything is flushed to disk.
		// Not sure if this is actually necessary.
		featureIdToName.syncDatabase();
	}

	
	public static List<Integer> getPageIds(int amount, int offset) {
		ArrayList<Integer> idList = new ArrayList<Integer>();
		int count = 0;

		try {
			FileReader fr = new FileReader(WikiIndexer.conf.get("wikiPageIds"));
			BufferedReader br = new BufferedReader(fr);
			String line = "";

			while ((line = br.readLine()) != null && idList.size() < amount) {
				if (count < offset) {
					count++;
				} else {
					idList.add(new Integer(line));				
				}
			}

			fr.close();
		}
		catch (IOException ex) {
			System.out.println("Could not read from file " + ex);
		}		

		return idList;
	}

	public static List<Integer> getPageIds() {
		return WikiIndexer.getPageIds(999999999, 0);
	}

	public void dumpDocIndex() throws IOException {
		DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex();
		docIdx.startTraversal();
		PostingsList p = null;

		File file = new File("docIdxDump.txt");

		// if file doesn't exist, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		int i = 0;
		while ((p = docIdx.getNext()) != null) {
			if (++i % 1000 == 0) System.out.println(i);
			bw.write(p.getKey()+"\t"+p.getPostings()+"\n");
		}
		bw.close();

		docIdx.endTraversal();
	}

	public void buildInvertedIndex() throws ItemRetrievalException {
		FeatureDocumentsIndex invertIdx = new FeatureDocumentsIndex("wikiW2VInvertedIdx");
		DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex("wikiW2VDocIdx");

		docIdx.startTraversal();
		PostingsList p;
		int i = 0;
		HashMap<Integer, PostingsList> store = new HashMap<Integer, PostingsList>();
		int cnt = 0;

		while ((p = docIdx.getNext()) != null) {
			if (++i % 5000 == 0) System.out.println(i);
			int docId = p.getKey();
			for (Map.Entry<Integer, Float> posting : p.getPostings().entrySet()) {
				cnt++;
				int featId = posting.getKey();
				float val = posting.getValue();

				// Look it up in the index. If it's there, add the new one.
				// If not, create a new list. Then re-add to the index.
				PostingsList featPost = null;

				if (store.containsKey(featId)) {
					featPost = store.get(featId);
				}
				else {
					featPost = new PostingsList(featId);
				}
				featPost.addItem(docId, val);	
				store.put(featId, featPost);
			}
			// roughly 1GB: put it in the db
			if (cnt > 100000000) {
				System.out.println(i +": Writing " + cnt + " to db");
				for (Map.Entry<Integer, PostingsList> entry : store.entrySet()) {
					invertIdx.merge(entry.getValue());
				}
				invertIdx.syncDatabase();
				store = new HashMap<Integer, PostingsList>();
				cnt = 0;
			}
		}

		System.out.println(i +": Writing " + cnt + " to db");
		for (Map.Entry<Integer, PostingsList> entry : store.entrySet()) {
			invertIdx.merge(entry.getValue());
		}
		invertIdx.syncDatabase();

		docIdx.endTraversal();

	}

	public void readInInvertedIndex() throws IOException {
		String fname = "/z/mrander/invIdx.txt";
		// Open the file
		FileInputStream fstream = new FileInputStream(fname);

		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String strLine;

		FeatureDocumentsIndex invertIdx = new FeatureDocumentsIndex();
		int cnt = 0;
		int insertCnt = 0;
		//Read File Line By Line
		while ((strLine = br.readLine()) != null)   {
			String[] parts = StringUtils.split(strLine, "\t");
			int featureId = new Integer(parts[0]);
			PostingsList p = new PostingsList(featureId);
			for (int i = 1; i < parts.length; i++) {
				String[] postingPair = StringUtils.split(parts[i], "=");
				int docId = new Integer(postingPair[0]);
				float val = new Float(postingPair[1]);
				p.addItem(docId, val);
			}
			if (p.getPostings().size() > 1) {
				invertIdx.put(p);
				insertCnt++;
				if (insertCnt % 10000 == 0) {
					System.out.println(p.getKey()+" "+p.getPostings());
					System.out.println(cnt);
					invertIdx.syncDatabase();
				}
			}
			cnt++;
		}
		invertIdx.syncDatabase();
		//Close the input stream
		in.close();
		System.out.println("Features: " + cnt + ", Inserted: " + insertCnt + ", Singletons: " + (cnt - insertCnt));
	}

	public int getNextId() {
		int next = -1;
		DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex();

		for (int pageId : WikiIndexer.getPageIds(2,0)) {
			PostingsList p;
			try {
				p = docIdx.get(pageId);
				System.out.println(pageId + " " + p.getKey() + ", Size: " + 
						p.getPostings().size());
			} catch (ItemRetrievalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return next;
	}

	public static void main(String[] args) {        
		WikiIndexer w = new WikiIndexer();

		try {
			//w.indexFileOffsets();
			//w.indexWikiPages(0);
			//w.importFeatureNames("test_data/wikipedia/feature_list.txt", "wiki500FeatureNameDict");
			w.buildInvertedIndex();
			//w.dumpDocIndex();
			//w.readInInvertedIndex();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		//w.getNextId();

	}
}
