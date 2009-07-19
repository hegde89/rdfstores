package edu.unika.aifb.graphindex.model;

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

import java.util.Collection;
import java.util.Set;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;

public interface IEntity  extends IResource {
	
	public void setType(INamedConcept type);
	
	public INamedConcept getType();
	
	public void setExtension(String extension);
	
	public String getExtension();
	
	public void addAttributeValueCompound(IAttributeValueCompound compound);
	
	public void setAttributeValueCompounds(Set<IAttributeValueCompound> compounds);
	
	public Set<IAttributeValueCompound> getAttributeValueCompounds();
	
	public void addAttributeValue(IValue value);
	
	public void setAttributeValues(Set<IValue> values);
	
	public Set<IValue> getAttributeValues();
	
}
