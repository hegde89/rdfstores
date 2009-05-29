package edu.unika.aifb.keywordsearch;



public class ElementComparator implements Comparable<KeywordElement> {

	public int compareTo(KeywordElement o) {
		if(this.equals(o))
			return 0;
		return 1;
	}
}
