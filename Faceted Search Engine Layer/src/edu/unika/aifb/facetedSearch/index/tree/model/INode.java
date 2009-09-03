/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
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
 * 
 */
package edu.unika.aifb.facetedSearch.index.tree.model;

import java.io.Serializable;
import java.util.List;

import edu.unika.aifb.facetedSearch.index.tree.model.impl.Node.NodeContent;
import edu.unika.aifb.facetedSearch.index.tree.model.impl.Node.NodeType;

/**
 * @author andi
 * 
 */
public interface INode extends Serializable {

	// public Set<INode> getChildren();

	public void addRangeExtension(String extension);

	public void addRangeExtensions(List<String> extensions);

	public NodeContent getContent();

	// public boolean hasChildren();

	public double getID();

	public String getValue();

	public List<String> getRangeExtensions();

	public NodeType getType();

	public void setContent(NodeContent content);

	public void setID(double id);

	public void setValue(String label);

	public void setRangeExtensions(List<String> extensions);

	public void setType(NodeType type);

}
