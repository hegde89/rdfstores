package edu.unika.aifb.graphindex.model.impl;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import edu.unika.aifb.graphindex.model.IAttribute;
import edu.unika.aifb.graphindex.model.IAttributeValueCompound;
import edu.unika.aifb.graphindex.model.IValue;

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
