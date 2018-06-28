package edu.umich.eecs.featext.DataSources;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;
import org.ejml.ops.NormOps;
import org.ejml.ops.RandomMatrices;

import com.google.common.base.Joiner;

import edu.umich.eecs.featext.index.DocumentFeaturesIndex;
import edu.umich.eecs.featext.index.PostingsList;

public class LSHClusterer {
	
	static DocumentFeaturesIndex docIdx = new DocumentFeaturesIndex("wiki500DocIdx");
	static String featureListFile = "featureDocCounts.txt";
	static int totalDocCount = 4174382;
	
	private int numBuckets;
	private Map<Integer, Double> featureIDF = new HashMap<Integer, Double>();
	private DenseMatrix64F randVectors; 
	
	private Map<Integer, List<Integer>> clusters = new HashMap<Integer, List<Integer>>();
	private Set<Integer> idSet;
	
	private Random rand = new Random();
	
	public LSHClusterer(int numBuckets) {
		this.numBuckets = numBuckets;
		
		try {
			loadFeatureList();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		randVectors = new DenseMatrix64F(numBuckets, featureIDF.size());
		createRandomVectors();
		
		try {
			idSet = new HashSet<Integer>(loadDocIds());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void loadFeatureList() throws FileNotFoundException {
		Scanner s = new Scanner(new File(featureListFile));

		while (s.hasNext()){
			String[] vals = s.nextLine().trim().split("\t");
			Integer featureId = Integer.valueOf(vals[0]);
			Double idf = Math.log(totalDocCount/Double.valueOf(vals[1]));
			featureIDF.put(featureId, idf);
		}

		s.close();
	}
	
	private void createRandomVectors() {
		// Create a random vector for each hash bucket, and make them unit vectors
		for (int i = 0; i < numBuckets; i++) {
			DenseMatrix64F v = RandomMatrices.createRandom(1, featureIDF.size(), 0.0, 1.0, rand);
			double norm = 0;
			
			for (int j = 0; j < featureIDF.size(); j++) {
				double g = rand.nextGaussian();
				v.set(j, g);
				norm += g * g;
			}
			
			
			CommonOps.divide(Math.sqrt(norm),v);
			CommonOps.insert(v, randVectors, i, 0);
		}
		
		// transpose it, since that's the form we'll use it
		CommonOps.transpose(randVectors);
	}

	public void hashDocs() throws IOException {
		docIdx.startTraversal();
		PostingsList p;		
		
		int docCount = 0;
		
		// to receive the hash
		DenseMatrix64F hashVec = new DenseMatrix64F(1, numBuckets);

		Long t1 = System.nanoTime();
		DenseMatrix64F v = new DenseMatrix64F(1, featureIDF.size());

		while ((p = docIdx.getNext()) != null) {
			int docId = p.getKey();
			if (!idSet.contains(docId)) continue;
			
			docCount++;
			if (docCount % 50000 == 0) System.out.println(docCount);
			v.zero();
			Map<Integer, Float> postings = p.getPostings();
			double norm = 0;
			
			for (Map.Entry<Integer, Float> posting : postings.entrySet()) {
				int featId = posting.getKey(); 
				double count = posting.getValue();
				double tfidf = Math.log(count + 1) * featureIDF.get(featId);
				featId--; // subtract 1 for indexed by zero

				norm += tfidf * tfidf;
				//System.out.println("-- " + (featId - 1) + "," + (numBuckets - 1) + " -- " + randVectors.numRows);
				
				// randVals is a vector containing the value at featId for each random vector
				DenseMatrix64F randVals = CommonOps.extract(randVectors, featId, featId+1, 0, numBuckets);

				// System.out.println(tfidf + ": " + hashVec + " " +  randVals);
				
				//Do: hashVec = hashVec + tfidf * randVals;
				CommonOps.addEquals(hashVec, tfidf, randVals);
			}
			
			// NOTE: if we want to actually compute similarity, we need to divide by the norm
			// But, it should be OK to not do it when just creating the hash.
			//CommonOps.divide(Math.sqrt(norm),hashVec);
	
			int clusterId = 0;

			for (int i = 0; i < numBuckets; i++) {
				clusterId += (hashVec.get(i) >= 0.0) ? (1 << i) : 0;
			}
			
			clusterId++; // start with 1
			
			if (!clusters.containsKey(clusterId)) clusters.put(clusterId, new ArrayList<Integer>());
			clusters.get(clusterId).add(docId);
		}

		Long t2 = System.nanoTime();
		
		System.out.println("Clustering time for " + docCount + " documents: " + (t2 - t1)/1000000000 + " s");
		
		docIdx.endTraversal();
	
		File file = new File("lsh-" + numBuckets + ".txt");
		// if file doesn't exist, then create it
		if (!file.exists()) {
			file.createNewFile();
		}
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);	
		for (Map.Entry<Integer, List<Integer>> cluster : clusters.entrySet()) {
				bw.write(cluster.getKey() + "\t" + Joiner.on(",").join(cluster.getValue()) + "\n");
		}
		bw.close();
	}
	
	private Map<Integer, List<Integer>> loadClusters() throws FileNotFoundException {
		Scanner s = new Scanner(new File("lsh-" + numBuckets + ".txt"));
		
		Map<Integer, List<Integer>> clusters = new HashMap<Integer, List<Integer>>();
		
		while (s.hasNext()){
			String[] vals = s.nextLine().trim().split("\t");
			
			Integer clusterId = Integer.valueOf(vals[0]);
			String[] docIds = vals[1].split(",");
			
			clusters.put(clusterId, new ArrayList<Integer>());
			
			for (String idStr : docIds) {
				clusters.get(clusterId).add(Integer.valueOf(idStr));
			}
			
			//System.out.println(clusterId + ": " + clusters.get(clusterId).size());
		}

		s.close();
		
		return clusters;
	}
	
	public List<Integer> loadDocIds() throws IOException {
		String binFile = "test_data/wikipedia/page_ids.bin";
		List<Integer> list = new ArrayList<Integer>();
		DataInputStream bin = new DataInputStream(new BufferedInputStream(new FileInputStream(binFile)));
		while (true) {
			try {
				list.add(bin.readInt());
			} catch(EOFException eof) {
				break;
			}
		}
		bin.close();
		return list;
	}
	
	public static void main(String[] args) {
		System.out.println("Clustering!");
		
		int numBuckets = Integer.valueOf(args[0]);
		
		LSHClusterer lsh = new LSHClusterer(numBuckets);
		
//		try {
//			Map<Integer, List<Integer>> clusters = lsh.loadClusters();
//			System.out.println(clusters.size());
//			List<Integer> idList = clusters.get(666);
//			List<WikiPage> pages = null;
//			try {
//				pages = WikiPage.loadFromFile(idList);
//			} catch (Exception e) {
//				e.printStackTrace();
//			} 
//
//			for (WikiPage wp : pages) {
//				System.out.println(wp.getTitle());// + "\n" + wp.getText() +"\n----\n");
//			}
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		try {
			lsh.hashDocs();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
