package edu.umich.eecs.featext.DataSources;

public class Question {
	int questionId;
	int[] entityIds;
	String question;
	String answer;
	String label;

	public Question(int id, String lineFromFile) {
		questionId = id;
		
		String[] data = lineFromFile.split("\t");
		entityIds = new int[data.length - 3];
		for (int i = 0; i < entityIds.length; i++) {
			entityIds[i] = Integer.parseInt(data[i]);
		}
		
		question = data[entityIds.length];
		answer = data[entityIds.length + 1];
		label = data[entityIds.length + 2];
//		if (label.equals("capital"))
//		System.out.println(entityIds[0] + " -- " + entityIds[1] + " -- " + questionId + " -- " + question + " -- " + answer + ", " + question.hashCode());
	}
	
	public int[] getEntities() {
		return entityIds;
	}
	
	public String getQuestion() {
		return question;
	}
	
	public String getAnswer() {
		return answer;
	}
	
	public String getLabel() {
		return label;
	}
}
