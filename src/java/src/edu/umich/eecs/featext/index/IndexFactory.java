package edu.umich.eecs.featext.index;


public class IndexFactory {
  public static DocumentFeaturesIndex docFeatIdx = new DocumentFeaturesIndex();
  //public static FeatureDict featureIdx = new FeatureDict();
  public static FeatureNameDict featureNamesIdx = new FeatureNameDict();
  public static FeatureDocumentsIndex featDocsIdx = new FeatureDocumentsIndex();
  //public static WikiFileOffsetDict offsetDict = new WikiFileOffsetDict();
}
