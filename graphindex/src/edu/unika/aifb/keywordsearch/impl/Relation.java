package edu.unika.aifb.keywordsearch.impl;

import edu.unika.aifb.keywordsearch.StructureGraphUtil;
import edu.unika.aifb.keywordsearch.api.IRelation;


public class Relation implements IRelation {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7385834589126876380L;
	private String uri;
	
	public Relation(String uri)	{
		this.uri = uri;
	}

	public String getLabel() {
		return StructureGraphUtil.getLocalName(uri);
	}

	public String getUri() {
		return uri;
	}
	
	public String toString() {
		return uri; 
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((uri == null) ? 0 : uri.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Relation other = (Relation)obj;
		if (uri == null) {
			if (other.uri != null)
				return false;
		} else if (!uri.equals(other.uri))
			return false;
		return true;
	}

}
