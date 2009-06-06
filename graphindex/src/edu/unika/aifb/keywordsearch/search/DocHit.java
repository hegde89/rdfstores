package edu.unika.aifb.keywordsearch.search;

public class DocHit{
	
	private int docId;
	private float score; 
	
	public DocHit(int docId, float score) {
		this.docId = docId;
		this.score = score;
	} 
	
	public int getDocId() {
		return docId;
	}
	
	public float getScore() {
		return score;
	}

}
