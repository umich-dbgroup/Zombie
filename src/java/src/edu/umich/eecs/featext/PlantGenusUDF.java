package edu.umich.eecs.featext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.*;

import edu.umich.eecs.featext.DataSources.WikiPage;

class PlantGenusUDF implements UDF {
        
    public List<String> execute(HashMap<String, String> input) {
        String genus = "";
        String text = (String) input.get("text");

        // First find the capital
        String pat = "\\|[\\s]*regnum[\\s]*=[\\s]\\[\\[Plantae\\]\\]";
        Pattern p = Pattern.compile(pat);
        Matcher m = p.matcher(text);

        while (m.find()) {
            pat = "\\|[\\s]*genus[\\s]*=[\\s]'''''([^']+)'''''";
            p = Pattern.compile(pat);
            m = p.matcher(text);

            while (m.find()) {
                genus = m.group(1);
            }
        }

        List<String> output = new ArrayList<String>();
        output.add(genus);

        return output;
    }
    
    public boolean test(HashMap<String, String> input) {
    	List<String> output = this.execute(input);
    	if (output.get(0) != "") {
    		return true;
    	}
    	else {
    		return false;
    	}
    }
    
    public String getName() {
    	return "genus";
    }
    
    public static void main(String[] args) {
		List<Integer> pageIds = new ArrayList<Integer>();
		pageIds.add(11448);
		pageIds.add(1217);
		pageIds.add(11867);
		pageIds.add(12);

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

		PlantGenusUDF udf = new PlantGenusUDF();
		
		for (WikiPage wp : pages) {
			HashMap<String, String> input = new HashMap<String, String>();
			input.put("pageId", String.valueOf(wp.getPageId()));
			input.put("title", wp.getTitle());
			input.put("text", wp.getText());
			
			System.out.println("----------------\n" + wp.getTitle());
			for (String entry : udf.execute(input)) {
				System.out.println(entry);
			}
		}
    }
}
