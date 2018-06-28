package edu.umich.eecs.featext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import edu.umich.eecs.featext.DataSources.WikiIndexer;
import edu.umich.eecs.featext.DataSources.WikiPage;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<Integer> ids = WikiIndexer.getPageIds(5000000, 0);
		
		//Collections.shuffle(ids);

		UDF udf = new BirthYearUDF();
		int cnt = 0;
		int found = 0;
		try {
			File file = new File("test_data/birth_inorder.txt");
			 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
 

			for (int id : ids) {
				cnt++;

				WikiPage wp = new WikiPage(id);
				HashMap<String, String> input = new HashMap<String, String>();			
				input.put("text", wp.getText());

				if (udf.test(input)) {
					found++;
					bw.write(cnt+"\t"+found+"\n");
				}


			}
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
//		FeatureDocumentsIndex invIdx = new FeatureDocumentsIndex();
//
//		try {
//			PostingsList p = invIdx.get(2);
//			System.out.println(p.getPostings());
//		} catch (ItemRetrievalException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

}
