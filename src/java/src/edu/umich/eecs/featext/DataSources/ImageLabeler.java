package edu.umich.eecs.featext.DataSources;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ImageLabeler {
	HashMap<Integer, String> imgLabels = new HashMap<Integer, String>();
	HashMap<String, ArrayList<Integer>> labelImgs = new HashMap<String, ArrayList<Integer>>();
	ArrayList<Integer> ids = new ArrayList<Integer>();

	public ImageLabeler(String fileList) {
		BufferedReader in;
		Set<String> toUse = new HashSet<String>();
		
		//toUse.add("camera");
		toUse.add("car_side");
		toUse.add("dolphin");
		toUse.add("electric_guitar");
		//toUse.add("euphonium");
		//toUse.add("strawberry");
		toUse.add("trilobite");
		toUse.add("water_lilly");
		toUse.add("wrench");
		//toUse.add("yin_yang");
		
		try {
			in = new BufferedReader(new FileReader(fileList));

			String curLine = null;
			int imgId = 0;
			while ((curLine = in.readLine()) != null) {
				imgId++;
				curLine = curLine.trim();
				String[] parts = curLine.split("/");
				
				//if (toUse.contains(parts[0])) {
					imgLabels.put(imgId, parts[0]);
					ids.add(imgId);

					if (!labelImgs.containsKey(parts[0])) {
						labelImgs.put(parts[0], new ArrayList<Integer>());
					}

					labelImgs.get(parts[0]).add(imgId);
				//}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	public String getLabel(int imgId) {
		return imgLabels.get(imgId);
	}

	public ArrayList<Integer> getImagesForLabel(String label) {
		return labelImgs.get(label);
	}

	public String[] getLabels() {
		String[] labels = new String[labelImgs.keySet().size()];
		int i = 0;
		for (String l : labelImgs.keySet()) {
			labels[i] = l;
			i++;
		}
		return labels;
	}

	public ArrayList<Integer> getIds() {
		return ids;
	}

	public static void main(String[] args) {
		ImageLabeler il = new ImageLabeler("test_data/Caltech101/imageFiles.txt");
		System.out.println(il.getLabels().length);
		System.out.println(il.getLabel(4000));

		String[] labels = il.getLabels();
		for (String l : labels) {
			System.out.println(l);
		}
		for (int i : il.getIds()) {
			System.out.println(i);
		}
	}
}
