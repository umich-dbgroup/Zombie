package edu.umich.eecs.featext.harness;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * @author Mike Anderson
 * Plotter: generates basic plot as a HTML file using Google Plots API
 */
public class Plotter {
	String dir = "test_data/plots/";
	String templatesDir = "templates/html/";
	
	public Plotter() {}

	public void makePlot(ArrayList<Double> xData, ArrayList<Double> yData, String templateName, String title, String xlabel, String ylabel) throws IOException {
		ArrayList<String> dataArr = new ArrayList<String>();
		dataArr.add(String.format("['%s', '%s']", xlabel, ylabel));
		
		for (int i = 0; i < xData.size(); i++) {
			dataArr.add(String.format("[%f, %f]", xData.get(i), yData.get(i))); 
		}
		
		String dataStr = StringUtils.join(dataArr, ",");
	
		String template = FileUtils.readFileToString(new File(templatesDir + templateName));

		template = template.replace("VAR_DATA", dataStr);
		template = template.replace("VAR_TITLE", title);
		template = template.replace("VAR_HAXIS", xlabel);
		template = template.replace("VAR_VAXIS", ylabel);

		String fname = title.replaceAll(" ", "_") + ".html";
		FileUtils.writeStringToFile(new File(dir + fname), template);

	}

}
