package edu.umich.eecs.featext.Policies;

import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umich.eecs.featext.DataSources.DataSource;

public class Item implements Comparable<Item> {
	Integer itemId;
	Writable contents;
	HashMap<Integer, Float> values = new HashMap<Integer, Float>();
	Long offset;
	Long length;
	int contentsSize = 0;
	boolean isInitialized = false;
	int blockId;
	String label;
	private Integer sortId = null;
	long loadTime = 0;
	
	DataSource data;
	private boolean shouldRandomizeFeatures = false;
	
	private static int currentFeatureId;
	
	public Item(int itemId) {
		this.itemId = itemId;
		Random rand = new Random();
		//rand.setSeed(2*itemId);
		sortId = rand.nextInt();
	}
	
	public Item(int itemId, DataSource data) {
		this(itemId);
		this.data = data;
	}
	
	public Item(int itemId, Writable contents, long offset, long length) {
		this.itemId = itemId;
		initialize(contents, offset, length);
	}
	
	public void initialize(Writable contents, long offset, long length) {
		this.contents = contents;
		if (contents != null)
			contentsSize = contents.toString().length();
		this.offset = offset;
		this.length = length;
		isInitialized = true;
	}

	public void initialize(String contents, long offset, long length) {
		this.contents = new Text(contents);
		if (contents != null)
			contentsSize = contents.toString().length();
		this.offset = offset;
		this.length = length;
		isInitialized = true;
	}
	
	public Integer getItemId() {
		return itemId;
	}
	
	public Writable getContents() {
		if (!isInitialized && data != null) {
			data.loadItem(this);
		}
		return contents;
	}
	
	public void setSortId(int sortId) {
		this.sortId = sortId;
	}
	
	public int getSortId() {
		return sortId;
	}
	
	public Float getValue(Integer featureId) {
		Float val = values.get(featureId);
		if (val == null) return (float) 0;
		else return values.get(featureId);
	}
	
	public void setDataSource(DataSource data) {
		this.data = data;
	}
	
	public void setValue(Integer featureId, Float value) {
		values.put(featureId, value);
	}
	
	public void addValue(Integer featureId, double d) {
		Float val = values.get(featureId);
		if (val == null) {
			values.put(featureId, (float) d);
		}
		else {
			values.put(featureId, (float) d + val);
		}
	}
	
	public HashMap<Integer, Float> getFeatures() {
		return values;
	}
	
	public Long getOffset() {
		return offset;
	}
	
	public Long getLength() {
		return length;
	}
	
	public void setBlockId(int blockId) {
		this.blockId = blockId;
	}
	
	public int getBlockId() {
		return blockId;
	}
	
	public void setLabel(String label) {
		this.label = label;
	}
	
	public String getLabel() {
		return label;
	}
	
	public boolean isInitialized() {
		return isInitialized;
	}
	
	public static void setCurrentFeature(int featureId) {
		currentFeatureId = featureId;
	}
	
	public String toString() {
		return "Item(" + itemId + ")";
	}
	
	public int size() {
		return contentsSize;
	}
	
	public long getLoadTime() {
		return loadTime;
	}
	
	public void setLoadTime(long t) {
		loadTime = t;
	}
	
	public void setShouldRandomizeFeatures(boolean randThem) {
		shouldRandomizeFeatures = randThem;
	}
	
	public boolean shouldRandomizeFeatures() {
		return shouldRandomizeFeatures ;
	}
	
  @Override
  // This should allow items to be sorted by value decreasing
  public int compareTo(Item otherItem){
      return otherItem.getValue(currentFeatureId).compareTo(this.getValue(currentFeatureId));
  }
}
