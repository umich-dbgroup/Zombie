package edu.umich.eecs.featext.Policies;

import java.util.HashMap;
import java.util.Map;

public class ItemCache {
	Map<Integer, Item> items = new HashMap<Integer, Item>();
	
	public ItemCache() { }
	
	public Item get(Integer itemId) {
		Item item = items.get(itemId);
		
		if (item == null) {
			item = new Item(itemId);
			items.put(itemId, item);
		}
		
		return item;
	}
	
	public Item remove(Integer itemId) {
		return items.remove(itemId);
	}
	
	public int size() {
		return items.size();
	}
}
