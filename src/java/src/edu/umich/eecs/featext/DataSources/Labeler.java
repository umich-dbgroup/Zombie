package edu.umich.eecs.featext.DataSources;

public interface Labeler {
	public String getLabel(int docId);
	public String[] getLabels();
	public void setDataSource(DataSource data);
}
