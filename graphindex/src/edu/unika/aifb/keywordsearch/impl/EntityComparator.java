package edu.unika.aifb.keywordsearch.impl;


public class EntityComparator implements Comparable<Entity> {

	public int compareTo(Entity o) {
		if(this.equals(o))
			return 0;
		return 1;
	}
}
