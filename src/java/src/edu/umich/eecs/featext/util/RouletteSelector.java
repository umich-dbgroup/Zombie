package edu.umich.eecs.featext.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RouletteSelector<T> {
	private List<T> items;
	private List<Double> probs;
	private Random rand = new Random();
	private Double normTerm;

	public RouletteSelector() {
		reset();
	}

	public void addItem(T item, Double prob) {
		items.add(item);
		probs.add(prob);
		normTerm += prob;
	}

	public T select() {
		Double p = rand.nextDouble();
		Double start = 0.0;
		T selectedItem = null;
		
		for (int i = 0; i < probs.size(); i++) {
			Double end = start + probs.get(i)/normTerm;
			if (p >= start && p < end) {
				selectedItem = items.get(i);
			}
			start = end;
		}
		
		return selectedItem;
	}
	
	public void reset() {
		items = new ArrayList<T>();
		probs = new ArrayList<Double>();
		normTerm = 0.0;
	}
}
