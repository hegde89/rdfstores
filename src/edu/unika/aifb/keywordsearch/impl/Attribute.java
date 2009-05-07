package edu.unika.aifb.keywordsearch.impl;

import edu.unika.aifb.keywordsearch.StructureGraphUtil;
import edu.unika.aifb.keywordsearch.api.IAttribute;


public class Attribute implements IAttribute {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1272100045208075248L;
	private String uri;
	
	public Attribute(String uri)	{
		this.uri = uri;
	}

	public String getLabel() {
		return StructureGraphUtil.getLocalName(uri);
	}

	public String getUri() {
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
		Attribute other = (Attribute) obj;
		if (uri == null) {
			if (other.uri != null)
				return false;
		} else if (!uri.equals(other.uri))
			return false;
		return true;
	}
	
}
