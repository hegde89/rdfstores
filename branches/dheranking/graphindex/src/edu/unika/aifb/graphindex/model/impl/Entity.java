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

import java.util.HashSet;
import java.util.Set;

import edu.unika.aifb.graphindex.model.IAttributeValueCompound;
import edu.unika.aifb.graphindex.model.IEntity;
import edu.unika.aifb.graphindex.model.INamedConcept;
import edu.unika.aifb.graphindex.model.IValue;
import edu.unika.aifb.graphindex.searcher.keyword.model.StructureGraphUtil;

public class Entity implements IEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3294446024824712138L;
	
	private String uri;
	private String extension;
	private INamedConcept type;
	private Set<IAttributeValueCompound> attributeValueCompounds;
	private Set<IValue> values;
	
	public Entity(String uri) {
		this.uri = uri;
	}
	
	public Entity(String uri, String extension) {
		this.uri = uri;
		this.extension = extension;
	}
	
	public String getLabel() {
		return StructureGraphUtil.getLocalName(uri);
	}

	public String getUri() {
		return uri;
	}
	
	public void setType(INamedConcept type) {
		this.type = type;
	}
	
	public INamedConcept getType() {
		return type;
	}
	
	public void setExtension(String extension) {
		this.extension = extension;
	}
	
	public String getExtension() {
		return extension;
	}
	
	public void addAttributeValueCompound(IAttributeValueCompound compound) {
		if(attributeValueCompounds == null) {
			attributeValueCompounds = new HashSet<IAttributeValueCompound>();
		}
		attributeValueCompounds.add(compound);	
	}
	
	public void setAttributeValueCompounds(Set<IAttributeValueCompound> compounds) {
		attributeValueCompounds = compounds;
	}
	
	public Set<IAttributeValueCompound> getAttributeValueCompounds() {
		return attributeValueCompounds;
	}
	
	public void addAttributeValue(IValue value) {
		if(values == null) {
			values = new HashSet<IValue>();
		}
		values.add(value);	
	}
	
	public void setAttributeValues(Set<IValue> values) {
		this.values = values;
	}
	
	public Set<IValue> getAttributeValues() {
		return values;
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
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Entity other = (Entity) obj;
		if (uri == null) {
			if (other.uri != null)
				return false;
		} else if (!uri.equals(other.uri))
			return false;
		return true;
	}

}
