package edu.unika.aifb.keywordsearch.impl;

import edu.unika.aifb.keywordsearch.api.IValue;

public class Value implements IValue {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8220989011365630410L;
	
	private String label;
	
	public Value(String label) {
		this.label = label;
	}

	public String getLabel() {
		return label;
	}

	public String getUri() {
		return getLabel();
	}

	public String toString() {
		return label; 
	}
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((label == null) ? 0 : label.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Value other = (Value) obj;
		if (label == null) {
			if (other.label != null)
				return false;
		} else if (!label.equals(other.label))
			return false;
		return true;
	}
	
}
