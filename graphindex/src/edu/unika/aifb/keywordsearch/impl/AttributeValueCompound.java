package edu.unika.aifb.keywordsearch.impl;

import edu.unika.aifb.keywordsearch.api.IAttribute;
import edu.unika.aifb.keywordsearch.api.IAttributeValueCompound;
import edu.unika.aifb.keywordsearch.api.IValue;

public class AttributeValueCompound implements IAttributeValueCompound {
	
	public AttributeValueCompound(IAttribute attribute, IValue value) {
		this.attribute = attribute;
		this.value = value;
	}
	
	private IAttribute attribute;
	private IValue value;
	
	public IAttribute getAttribute() {
		return attribute;
	}

	public IValue getValue() {
		return value;
	}
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + 7*attribute.hashCode() + 11*value.hashCode();
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AttributeValueCompound other = (AttributeValueCompound)obj;
		if (!attribute.equals(other.getAttribute())) {
				return false;
		} else if (!value.equals(other.getValue()))
			return false;
		return true;
	}

}
